from ncp_inputs import (get_oferta_hidro_diario, get_cotas_diario, restricciones_forecast_diario,
                        get_forecast_caudal, get_demanda, get_precios_combustible, get_eolicas,
                        con_iniciales_termicas, generacion_meta, con_iniciales_hidros, rampas,
                        margen_rro, capacidad_instalada, volumen_alerta, volumen_cota, restricciones_forecast_diario_v2,
                         get_oferta_hidro_diario_cvg_sem, get_precios_combustible_v2, get_precios_rro)
from ncp_dat import (get_thermals_in_configuration, crear_caudcp,
                     crear_condih, crear_cppmhigu, crear_cpresgGU, crear_cppmtrgu, crear_cprgergu)
from ncp_mantenimientos import mantenimiento, get_start_and_end_date_from_calendar_week
from ncp_inputs_emergencia import missing_inputs_alarm
from ncp_union_procesos import POST_NCP_PROCESS
from ncp_plantas_activas import plantas_activas
from ncp_notification_mail import send_notification
from ncp_notifications_teams import send_message_teams
from ncp_input_verification import read_input_verification_file, write_input_verification_file
import json
import shutil
import os
from datetime import datetime, timedelta
import logging
import requests
import numpy as np
import time
import pandas as pd
import concurrent.futures
import subprocess
import win32api
import winsound
from dotenv import load_dotenv
from pathlib import Path
from ncp_envio_resultado import send_final_results
from joblib import Parallel, delayed
import traceback
from os.path import exists
import sys

load_dotenv()  # take environment variables from .env
GTM_API_TEAMS = os.getenv('GTM_API_TEAMS_ERROR')
duration = 1000  # millisecondss
freq = 440  # Hz


def get_json_configuration_case(config: dict):
    """Crea el diccionario necesario para la seccion del ncp gui
        ->Run
            -->Execution parameters

    :param config: diccionario con los parametros necesarios
                   para la configuracion del caso
    :type config: dict
    :return: diccionario con la configuracion base estadar para un caso
    :rtype: list of dict
    """

    idioma = config.get('config').get('idioma')

    start = config.get('config').get('fechai').strftime('%Y-%m-%d')

    hora_ini = config.get('config').get('horai')

    hora_max = config.get('config').get('horaf')

    study_config = {"Idioma": idioma,
                    "Rede": 0,
                    "ShortTerm": {
                        "InitialDate": start,
                        "InitialHour": hora_ini,
                        "NumberHours": hora_max,
                        "StageDuration": 1,
                        "SolutionMethod": 0,
                        "Presolve": 1,
                        "MaxTime": 30,
                        "Tolerance": 1.0,
                        "HeuristicEnfasis": 0,
                        "Description": "ELECTRIC POWER MARKETS"
                    }
                    }
    return [study_config]


def get_json_oferta_hidro(day, hours, code):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->Hydros plants
                -->Energy bid

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: dict
    """
    code = True
    # out, warnings = get_oferta_hidro_diario(day, hours, code)
    out, warnings = get_oferta_hidro_diario_cvg_sem(day, hours, code)
    out = out.sort_values(['generadora', 'fecha'])

    horizonte = [out['fecha'].min(), out['fecha'].max()]

    filter = "code" if code else "name"

    result = map(lambda x:
                 {
                     "query_filter": {filter: int(x) if filter == 'code' else x},
                     "EnergyBid": {
                         "hourly_horizon": horizonte,
                         "data": list(out[(out.generadora == x)]['costo'])
                     }
                 },
                 list(out.generadora.unique())
                 )

    return list(result)


def get_json_cotas(day, hours, code):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Configuration
            -->Hydros plants
                -->Hidro configuration
                    ->Reservoir:Initial Condition

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: dict
    """
    out = get_cotas_diario(day, hours, code)
    filter = "code" if code else "name"
    by = "cod_turbina" if code else "nemo"
    result = map(lambda x:
                 {
                     "query_filter": {filter: int(x) if filter == 'code' else x},
                     "Vinic": float(out[out[by] == x]['cota']),
                     "VinicType": 1
                 },
                 list(out[by].unique())
                 )
    return list(result)

def get_json_rro(day, hours):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->Hydros plants
                -->Energy bid

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: dict
    """
    def generar_json(tipo_codigo):
        
        code = True
        out = get_precios_rro(day, hours)
        out = out[["FECHA", tipo_codigo, "PRECIO" ]].dropna()
        out = out.sort_values([tipo_codigo, 'FECHA'])
        out['ConvertedDate'] = out['FECHA'].dt.strftime('%Y-%m-%d %H:%M')
        horizonte = [out['ConvertedDate'].min(), out['ConvertedDate'].max()]

        filter = "code" if code else "name"
        print(tipo_codigo)
        result = map(lambda x:
                     {
                         "query_filter": {filter: int(x) if filter == 'code' else x},
                         "SSR_BidPriceBoth": {
                             "hourly_horizon": horizonte,
                             "data": list(out[(out[tipo_codigo] == x)]['PRECIO'])
                         }
                     },
                     list(out[tipo_codigo].unique())
                     )
        return result

    return list(generar_json("COD_TURBINA")), list(generar_json("COD_TERMICA"))


def condicion_inicial_rampas(mode: bool, pais):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->Thermal plants
                -->Operational constraints
                    ->Selected constraints

    :param mode: True si es una corrida diaria, False si es semanal
    :type mode: bool
    :param pais: configuracion del pais para el caso
    :type pais: string
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: list of dict
    """
    condicion_inicial = {"query_filter": {"name": pais},
                         "ThermalPlant_OperationalConstraints_UseMinimumUpTime": 0 if mode else 1,
                         "ThermalPlant_OperationalConstraints_UseMinimumDownTime": 0 if mode else 1,
                         "ThermalPlant_OperationalConstraints_UseMaximumRampUp": 1 if mode else 0,
                         "ThermalPlant_OperationalConstraints_UseMaximumRampDown": 1 if mode else 0,
                         "ThermalPlant_MaintenanceValueUnit": 3,
                         "HydroPlant_MaintenanceValueUnit": 3,
                         "ReserveGenerationValueUnit": 3
                         }
    return [condicion_inicial]


def get_json_restricciones_generacion(day, hours, hour, code):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->Systemn
                -->Generation contraints

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: list of dict
    """
    def sign_validation(sign):
        if sign == "=":
            return 1
        elif sign == ">":
            return 2
        else:
            return 1

    # out, warnings, nemos_restri = restricciones_forecast_diario(
    #     day, hours, hour, code)  # table='GUA_FORECAST_GENERACION_NCP_BACKTEST'
    out, warnings, nemos_restri = restricciones_forecast_diario_v2(
        day, hours, hour, code)
    print(warnings)
    filter = "code" if code else "name"
    by = "cod_restriccion" if code else "nemo"
    out = out.sort_values([by, 'datetime'])
    # out['sign'] = 1

    out['sign'] = out['TipoDespacho'].apply(sign_validation)
    # se agrega el segundo grupo de data con >4
    # out = out[(out['nemo'] != 'CHX-H >')]
    greater_than = out.copy()
    greater_than = greater_than[(greater_than['nemo'] != 'CHX-H')]
    greater_than_sign = ">"
    greater_than[f"{by}"] = greater_than[f"{by}"].apply(
        lambda x: x + f" {greater_than_sign}")
    greater_than['sign'] = greater_than['sign'].apply(
        lambda x: 2 if x == 1 else 2)

    out = pd.concat([out, greater_than], ignore_index=False)
    # out.to_csv('restri_x_than.csv', index=False)
    horizonte = [out['datetime'].min(), out['datetime'].max()]

    result = map(lambda x:
                 {
                     "query_filter": {filter: int(x) if filter == 'code' else x},
                     #  "sign": "<" if (out[(out[by] == x)]['sign']).all() == 1 else ">",
                     "ShortTermLimit": {
                         "hourly_horizon": horizonte,
                         "data": list(out[(out[by] == x)]['restricciones'])
                     }
                 },
                 list(out[by].unique())
                 )
    return list(result), out, nemos_restri


def get_json_cauales_hidro(day, hours, code):
    """Obtiene el frame necesario para la seccion del ncp gui
        aca se modela como central las hidros
        ->Scenarios/Contraints
            ->Hydros plants
                -->Inflow forecast(n) : n = numero de escenario

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: list of dict
    """
    code = True
    out, warnings, escenario = get_forecast_caudal(day, hours, code)
    print(warnings)
    filter = "code" if code else "name"
    by = "cod_embalse" if code else "nemo"
    out = out.sort_values([by, 'fecha'])
    horizonte = [out['fecha'].min(), out['fecha'].max()]
    result = map(lambda x:
                 {
                     "query_filter": {filter: int(x) if filter == 'code' else x},
                     "InflowForecast(1)": {
                         "hourly_horizon": horizonte,
                         "data": list(out[(out[by] == x)]['caudal'])
                     }
                 },
                 list(out[by].unique())
                 )
    # jsonString = json.dumps(result_json, indent=4)
    return list(result), escenario


def get_json_demanda(day, hours, tipo, pais):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->System
                -->Load

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param tipo: Tipo a demanda a generar : yhat, yhat_lower, yhat_upper
    :type tipo: string
    :param pais: pais al cual se aplicara esta demanda
    :type pais: string
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: dict
    """
    if 'wrf' in tipo:
        tipo = tipo.split('_')
        if len(tipo) > 2:
            tipo = "_".join(tipo[:2])
        else:
            tipo = tipo[0]
        print(tipo)
        out, warnings, CF_pro, escenario = get_demanda(
            day, hours, tipo, tabla='GUA_FORECAST_DEMANDA_WRF', version='xgb_wrf_v1')
    else:
        out, warnings, CF_pro, escenario = get_demanda(day, hours, tipo)
      # tabla='GUA_FORECAST_DEMANDA_WRF', version='xgb_wrf_v1'
    horizonte = [out['datetime'].min(), out['datetime'].max()]
    result = [{"query_filter": {"name": pais}, "ShortTermDemand": {"60min_horizon": horizonte,
                                                                   "data": list(out.demanda)}}]
    return result, CF_pro, escenario


def get_json_precios_combustible(day, hours, code):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->Fuels
                -->Fuel price

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: list of dict
    """
    # out, warnings = get_precios_combustible(day, hours, code)
    out, warnings = get_precios_combustible_v2(day, hours, 8, code)
    # out.to_csv('precio_combustibles.csv', index=False)
    filter = "code" if code else "name"
    by = "cod_combustible" if code else "nemo"
    horizonte = [out['fecha'].min(), out['fecha'].max()]
    result = map(lambda x:
                 {"query_filter": {filter: int(x) if filter == 'code' else x},
                  "ShortTermPrice": {"hourly_horizon": horizonte, "data": list(out[(out[by] == x)]['precio'])}
                  },
                 list(out[by].unique())
                 )
    return list(result)


def get_json_eolicas(day, hours, code):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->Renewable sources
                -->Generation (n) : n = numero de scenario

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: list of dict
    """
    out, warnings = get_eolicas(day, hours, code)

    filter = "code" if code else "name"
    by = "cod_eolico" if code else "nemo"
    out = out.sort_values([by, 'datetime'])
    horizonte = [out['datetime'].min(), out['datetime'].max()]
    result = map(lambda x:
                 {
                     "query_filter": {filter: int(x) if filter == 'code' else x},
                     "ShortTermGeneration(1)": {
                         "60min_horizon": horizonte,
                         "data": list(out[(out[by] == x)]['pred_generacion'])
                     }
                 },
                 list(out[by].unique())
                 )
    return list(result)


def get_json_con_termicas(day, code):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->Thermal plants
                -->Initial Status

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: dict
    """
    code = False
    out = con_iniciales_termicas(day, code)
    filter = "code" if code else "name"
    by = "!Code"
    out = out.sort_values([by])
    result = map(lambda x:
                 {
                     "query_filter": {filter: int(x) if filter == 'code' else x},
                     "InitialStatus": int(out[(out[by] == x)]['Status[0:Off/1:On]']),
                     "NumberOfHours": int(out[(out[by] == x)]['TIME[h]']),
                     "PreviousGeneration": float(out[(out[by] == x)]['PrevGeneration[MW]']),
                     "PeriodWithConstantPower":
                     int(out[(out[by] == x)]['PeriodContPower[h]']) *
                     -1 if int(out[(out[by] == x)]
                               ['Previous load condition[1:Power Increase/-1:Power Decrease]']) < 0 else 1
                 },
                 list(out[by].unique())
                 )
    return list(result)


def get_json_con_hidros(day, code):
    out = con_iniciales_hidros(day, code)
    filter = "code" if code else "name"
    by = "cod_turbina" if code else "nemo"
    out = out.sort_values([by])
    result = map(lambda x:
                 {"query_filter": {filter: int(x) if filter == 'code' else x},
                  "InitialStatus": int(out[(out[by] == x)]['on']),
                  "NumberOfHours": int(out[(out[by] == x)]['horas']),
                  "PreviousGeneration": float(out[(out[by] == x)]['aporte'])
                  },
                 list(out[by].unique())
                 )
    return list(result)


def get_json_generacion_meta(day, hours):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->System
                -->Target generation

    :param day: dia inicial para la generacion de horas
    :type day: int
    :return: retorna generacion meta de 'CHIXOY' y 'JURÚN MARINALÁ' por hora
    :rtype: list of dict
    """
    out = generacion_meta(day, hours)
    result = map(lambda x:
                 {
                     "query_filter": {"code": int(x)},
                     "LB": 0,
                     # int(out[(out.PLANTA == x)]['LOWER']),
                     "UB": int(out[(out.PLANTA == x)]['UPPER']),
                     "InitialHour": 1 if (int(x) == 1) or (int(x) == 3) else 25,
                     "FinalHour": 48 if (int(x) == 2) or (int(x) == 4) else 24
                 },
                 list(out.PLANTA.unique())
                 )
    return list(result)


def get_json_rampas(code, forecast):
    """Obtiene el frame necesario para la seccion del ncp gui
        ->Scenarios/Contraints
            ->Thermal plants
                -->Operational constraints

    :return: retorna un diccionario con el formato correcto para esta seccion
    :rtype: list of dict
    """
    print("::::Este es el forecast desde rampas:::\n", forecast)
    nemos_forescast = forecast.nemo.unique()
    status_nemos_forecast = pd.DataFrame()
    for nemo in nemos_forescast:
        restricion = forecast[(forecast.nemo == nemo)]
        if (restricion.restricciones == 0).all():
            status_nemos_forecast = pd.concat(
                [status_nemos_forecast, pd.DataFrame({"nemo": [nemo], "status":[bool(True)]})])
        elif(restricion.restricciones >= 0).any():
            status_nemos_forecast = pd.concat(
                [status_nemos_forecast, pd.DataFrame({"nemo": [nemo], "status":[bool(False)]})])

    # forecast.to_csv('forecast.csv', index=False)
    # status_nemos_forecast.to_csv('status_forecast.csv', index=False)
    out = rampas(code)
    # out.to_csv('rampas_antes.csv', index=False)
    out = pd.merge(out, status_nemos_forecast, left_on="NEMO",
                   right_on="nemo", how="left")
    out["status"] = np.where(pd.isna(out["status"]), bool(True), out["status"])

    out = out[['NEMO', 'RAMPA_BAJADA', 'RAMPA_SUBIDA', 'status']]
    # out.to_csv('rampas_pre_final.csv', index=False)

    out = out[out.status]
    # out.to_csv('rampas_final.csv', index=False)
    filter = "code" if code else "name"
    by = 'CODIGO' if code else 'NEMO'
    result = map(lambda x:
                 {
                     "query_filter": {filter: int(x) if filter == 'code' else x},
                     "MaximumRampUp": float(out[(out[by] == x)]['RAMPA_SUBIDA']),
                     "MaximumRampDown": float(out[(out[by] == x)]['RAMPA_BAJADA'])
                 },
                 list(out[by].unique())
                 )
    return list(result), out


def get_json_config_termicas(BASE_DIR):
    thermals = get_thermals_in_configuration(BASE_DIR)

    def validate(thermal):
        mex = thermal.split('-')
        if 'MEX' in mex[0] and mex[1] != 'I2':
            record = {"query_filter": {"name": thermal}, "Existing": [1]}
            return record
        elif 'ESA-IMP' in thermal or 'HON-IMP' in thermal:
            record = {"query_filter": {"name": thermal}, "Existing": [1]}
            return record
        elif 'ESA-I' in thermal or 'IMP-01' in thermal:
            record = {"query_filter": {"name": thermal},
                      "Existing": [0], "GerMax": [310]}
            return record
        elif 'EDC-I' in thermal or 'MEX-I2' in thermal or 'MEX-I3' in thermal:
            record = {"query_filter": {"name": thermal},
                      "Existing": [0], "GerMax": [120]}
            return record
    result = map(lambda x: validate(x), thermals)
    cleaned = filter(lambda x: x, result)
    return list(cleaned)


def get_json_margen_rro(day, hours, tipo):
    # tabla='GUA_FORECAST_DEMANDA_WRF', version='xgb_wrf_v1'
    if 'wrf' in tipo:
        tipo = tipo.split('_')
        if len(tipo) > 2:
            tipo = "_".join(tipo[:2])
        else:
            tipo = tipo[0]
        print(tipo)
        out = margen_rro(
            day, hours, tipo, tabla='GUA_FORECAST_DEMANDA_WRF', version='xgb_wrf_v1')
    else:
        out = margen_rro(day, hours, tipo)
    horizonte = [out['datetime'].min(), out['datetime'].max()]
    codigo = [1, 2]
    result = map(lambda x:
                 {
                     "query_filter": {"code": x},
                     "ShortTermReserve": {"60min_horizon": horizonte,
                                          "data": list(out['margen'])}
                 },
                 codigo
                 )
    return list(result)


def get_json_mantos_hydros(mantos):
    out = mantos
    horizonte = [out['FECHA'].min(), out['FECHA'].max()]
    result = map(lambda x:
                 {
                     "query_filter": {"name": x},
                     "MaintenanceValue": {"60min_horizon": horizonte,
                                          "data": list(out[(out.NEMO == x)]['MWH'])}
                 },
                 list(out.NEMO.unique())
                 )
    return list(result)


def get_json_mantos_thermal(mantos):
    out = mantos
    horizonte = [out['FECHA'].min(), out['FECHA'].max()]
    result = map(lambda x:
                 {
                     "query_filter": {"name": x},
                     "MaintenanceValue": {"60min_horizon": horizonte,
                                          "data": list(out[(out.NEMO == x)]['MWH'])}
                 },
                 list(out.NEMO.unique())
                 )
    return list(result)


def get_json_mantenimientos(day, hour, demanda, forecast):
    """extrae los mantenimientos para el dia solicitado

    :param day: _description_
    :type day: _type_
    :param hour: _description_
    :type hour: _type_
    :param demanda: _description_
    :type demanda: _type_
    :param forecast: forecast de generacion para actualizar spc
    :type forecast: dataframe
    :return: _description_
    :rtype: _type_
    """
    forecast = forecast.groupby(['nemo'], sort=False, as_index=False)[
        'restricciones'].max()

    # forecast.to_csv('forecast_from_json_mantos.csv', index=False)
    mantos = mantenimiento(day, hour, demanda)
    # mantos.to_csv('mantos_original.csv', index=False)
    for index, row in forecast.iterrows():
        mantos['MWH'] = np.where((mantos.NEMO == row.nemo) & (
            mantos.MWH != 0), row.restricciones, mantos.MWH)

    # mantos.to_csv('mantos_update_forecast.csv', index=False)
    thermals = ['GENERADOR DISTRIBUIDO RENOVABLE TERMICO', 'GEOTERMICA', 'INGENIOS AZUCAREROS', 'MOTORES DE COMBUSTION INTERNA',
                'PLANTA CICLO COMBINADO', 'TRANSACCION INTERNACIONAL', 'TURBINAS DE GAS', 'TURBINAS DE GAS NATURAL', 'TURBINAS DE VAPOR',
                'FOTOVOLTAICA']
    hydro = mantos[(mantos.tecnologia == 'HIDROELECTRICA')]
    thermal = mantos[(mantos.tecnologia.isin(thermals))]
    return hydro, thermal


def get_json_c_installed():
    capacidad = capacidad_instalada()
    # capacidad.to_csv('capacidad_instalada.csv', index=True)
    thermals = ['GENERADOR DISTRIBUIDO RENOVABLE TERMICO', 'GEOTERMICA', 'INGENIOS AZUCAREROS', 'MOTORES DE COMBUSTION INTERNA',
                'PLANTA CICLO COMBINADO', 'TRANSACCION INTERNACIONAL', 'TURBINAS DE GAS', 'TURBINAS DE GAS NATURAL', 'TURBINAS DE VAPOR',
                'FOTOVOLTAICA']
    hydros = capacidad[(capacidad.tecnologia == 'HIDROELECTRICA')]
    thermal = capacidad[(capacidad.tecnologia.isin(thermals))]
    return hydros, thermal


def get_json_c_hydro(capacidad):
    print(capacidad)
    result = map(lambda x:
                 {
                     "query_filter": {"name": x},
                     "PotInst": list(capacidad[(capacidad.nemo_ncp == x)]['capacidad_max']),
                     "MinimumGeneration": float(capacidad[(capacidad.nemo_ncp == x)]['Pmin'])
                 },
                 list(capacidad.nemo_ncp.unique())
                 )
    return list(result)


def get_json_c_thermal(capacidad, forecast):
    # forecast = forecast[(forecast.restricciones > 0).any()]
    # forecast.to_csv('forecast_from_capacidad_termica.csv', index=False)
    forecast = forecast.groupby(
        'nemo', as_index=False).restricciones.agg([min, max])
    # x = forecast.groupby('nemo', as_index=False)['restricciones'].agg(Min='min', Max='max')
    # forecast.to_csv('forecast_from_capacidad_termica_x.csv', index=True)
    capacidad = pd.merge(capacidad, forecast, left_on='nemo_ncp',
                         right_index=True, how='left')

    capacidad['capacidad_max'] = np.where(
        pd.isna(capacidad['max']), capacidad['capacidad_max'], capacidad['max'])
    capacidad['Pmin'] = np.where(
        pd.isna(capacidad['min']), capacidad['Pmin'], capacidad['min'])
    # capacidad.to_csv('capacidad.csv', index=False)
    result = map(lambda x:
                 {
                     "query_filter": {"name": x},
                     "GerMax": list(capacidad[(capacidad.nemo_ncp == x)]['capacidad_max']),
                     "GerMin": list(capacidad[(capacidad.nemo_ncp == x)]['Pmin'])
                 },
                 list(capacidad.nemo_ncp.unique())
                 )
    return list(result)


def get_json_volumen_alerta(day, hours):
    out = volumen_alerta(day, hours)
    horizonte = [out['FECHA'].min(), out['FECHA'].max()]
    result = map(lambda x: {"query_filter": {"name": x},
                            "NCPAlertStorage": {"hourly_horizon": horizonte,
                                                "data": list(out[(out.NEMO == x)]['va'])}
                            },
                 list(out.NEMO.unique())
                 )
    return list(result)


def get_json_plantas_activas(day, hour, rampa, mantenimientos):
    # condi_ini = con_iniciales_termicas(day, False)
    # # 'PrevGeneration[MW]','Pmin','capacidad_max'
    # condi_ini.loc[(condi_ini['Pmin'] > condi_ini['PrevGeneration[MW]']) & (
    #     condi_ini['Status[0:Off/1:On]'] == 1), 'condicioni'] = bool(True)
    # condi_ini = condi_ini[['!Code', 'Status[0:Off/1:On]', 'condicioni']]
    # condi_ini = condi_ini.rename(
    #     columns={'!Code': 'nemo_ncp', 'Status[0:Off/1:On]': 'status'})
    # condi_ini['status'] = np.where(
    #     condi_ini['status'] == 1, bool(True), bool(False))

    # condi_ini.to_csv('condi_ini.csv', index=True)
    despacho = plantas_activas(day, hour)
    thermals = ['INGENIOS AZUCAREROS', 'TURBINAS DE VAPOR']
    despacho = despacho[(despacho.tecnologia.isin(thermals))]
    despacho = despacho[['nemo_ncp', 'tecnologia', 'status']]
    # despacho.to_csv(f'despacho_{day.date()}.csv', index=True)
    # despacho = pd.merge(despacho, condi_ini, left_on='nemo_ncp',
    #                     right_on='nemo_ncp', how='outer')
    # despacho.dropna(subset=['tecnologia'], inplace=True)
    # despacho['status'] = np.where(
    #     pd.isna(despacho['status_y']), despacho['status_x'], np.nan)
    # despacho['status'] = np.where((despacho['status_x'] == True) & (
    #     despacho['status_y'] == False), despacho['status_y'], despacho['status_x'])

    # despacho.to_csv(f"despacho_{day.date()}_v1.csv", index=False)
    despacho = despacho[['nemo_ncp', 'tecnologia', 'status']]
    rampa = rampa[['NEMO', 'status']]
    rampa = rampa.rename(columns={'NEMO': 'nemo_ncp', 'status': 'rampas'})
    despacho = pd.merge(despacho, rampa, left_on='nemo_ncp',
                        right_on='nemo_ncp', how='outer')
    # despacho.dropna(subset=['rampas'], inplace=True)
    despacho['mantos'] = np.nan
    mantenimientos = mantenimientos[(
        mantenimientos.NEMO.isin(despacho.nemo_ncp.unique()))]
    if not mantenimientos.empty:
        mantenimientos['FECHA'] = pd.to_datetime(
            mantenimientos['FECHA'], format='%Y-%m-%d %H:%M')
        mask = (mantenimientos['FECHA'] >= day) & (
            mantenimientos['FECHA'] <= (day + timedelta(hours=48)))
        mantenimientos = mantenimientos.loc[mask]
        for nemo in mantenimientos.NEMO.unique():
            record = mantenimientos[(mantenimientos.NEMO == nemo)]
            if (record.MWH == 0).all():
                print(f"El nemo {nemo} no tiene mantos en las 48 horas")
                mask = (despacho.nemo_ncp == nemo)
                despacho.loc[mask, 'mantos'] = bool(False)
            elif (record.MWH >= 0).any():
                hora = record[(record.MWH > 0)].sort_values(
                    by=['FECHA']).head(1)
                print(f"El nemo {nemo} si tiene mantos")
                if hora is not None:
                    new_record = record.loc[(
                        record.FECHA >= hora.FECHA.to_string(index=False))]
                    if (new_record.MWH > 0).all():
                        print(
                            f"El nemo {nemo} si tiene mantos en las 48 horas")
                        print(hora)
                        mask = (despacho.nemo_ncp == nemo)
                        despacho.loc[mask, 'mantos'] = bool(True)
                    else:
                        print(
                            f"El nemo {nemo} si no tiene mantos en las 48 horas, sino tiene mantenimientos parciales")
                        print(hora)
                        mask = (despacho.nemo_ncp == nemo)
                        despacho.loc[mask, 'mantos'] = bool(False)
    # despacho.loc[(despacho.status) & (despacho.rampas) & (despacho.condicioni) & ((despacho.mantos) | (pd.isna(despacho.mantos))),
    #              'commitment'] = bool(True)
    despacho.loc[(despacho.status) & (despacho.rampas) & ((despacho.mantos) | (pd.isna(despacho.mantos))),
                 'commitment'] = bool(True)

    despacho.loc[(despacho.status) & (despacho.rampas) & (despacho.mantos is False),
                 'commitment'] = bool(False)
    # despacho.loc[(despacho.status) & (despacho.rampas) & (despacho.condicioni) & (despacho.mantos is False),
    #              'commitment'] = bool(False)

    despacho.loc[(despacho.status) & ((despacho.rampas) | pd.isna(despacho.rampas)) & ((despacho.mantos) | (pd.isna(despacho.mantos))),
                 'commitment'] = bool(True)
    despacho.loc[(despacho.status) & ((despacho.rampas) | pd.isna(despacho.rampas)) & (despacho.mantos is False),
                 'commitment'] = bool(False)
    despacho.loc[(pd.isna(despacho.commitment)),
                 'commitment'] = bool(True)

    # mask = (despacho.nemo_ncp == 'SJO-C')
    # despacho.loc[mask, 'commitment'] = bool(False)

    # mask = (despacho.nemo_ncp == 'SJO-C')
    # despacho.loc[mask, 'status'] = bool(False)

    # despacho['commitment'] = np.nan
    # mask = (despacho.status is True) & (
    #     despacho.condicioni is True) & (despacho.rampas is True) & (despacho['mantos'] is True)
    # despacho.loc[mask, 'commitment'] = bool(True)

    # despacho.to_csv(f"despacho_{day.date()}_v2.csv", index=False)

    # result = map(lambda x: 1 if despacho[(despacho.nemo_ncp == x)]['status'].bool() else 0, list(despacho.nemo_ncp.unique()))
    # despacho.to_csv(f'despacho_{day.date()}_final.csv', index=True)
    result = map(lambda x: {"query_filter": {"name": x}, "Existing": [0] if despacho[(
        despacho.nemo_ncp == x)]['status'].bool() else [1], "ComT": 2 if despacho[(despacho.nemo_ncp == x)]['commitment'].bool() else 0}, list(despacho.nemo_ncp.unique()))
    # , "ComT": 0 if despacho[(despacho.nemo_ncp == x)]['status'].bool() else 1}, list(despacho.nemo_ncp.unique()))
    # print(list(result))
    return list(result)


def get_json_constraint_type(nemos_restricciones, forecast):
    greater_than = ">"

    # 1 es <
    nemos_restricciones['sign'] = 1
    nemos_restricciones_mt = nemos_restricciones.copy()
    # 2 es >
    nemos_restricciones_mt['sign'] = 2
    nemos_restricciones_mt['nemo'] = nemos_restricciones_mt['nemo'].apply(
        lambda x: x + f" {greater_than}")
    nemos_restricciones = pd.concat(
        [nemos_restricciones, nemos_restricciones_mt], ignore_index=True)
    nemos_restricciones['sign'] = np.where(
        (nemos_restricciones.tecnologia == 'INGENIOS AZUCAREROS'), 2, nemos_restricciones.sign)
    nemos_restricciones['type'] = 'base'
    # forecast.to_csv('forecast_from_contraint.csv', index=True)
    # del forecast se valida si todo el horizonte por nemo es 0 es por que podra generar a full
    nemos_forecast = forecast[['nemo', 'tecnologia', 'sign']]
    nemos_forecast.drop_duplicates(inplace=True)
    nemos_forecast['type'] = 'forecast'
    nemos_forecast.drop_duplicates(subset=['nemo'], keep='first', inplace=True)
    # for nemo in forecast.nemo.unique():
    #     nemo_in_forecast = forecast[(forecast.nemo == nemo)]
    #     if (nemo_in_forecast.tecnologia.all() == 'INGENIOS AZUCAREROS') and ((nemo_in_forecast.restricciones == 0).all()):
    #         mask = (nemos_forecast.nemo == nemo)
    #         nemos_forecast.loc[mask, 'sign'] = 2

    join = pd.concat([nemos_restricciones, nemos_forecast], ignore_index=True)

    join['type'] = join['type'].astype('category').cat.set_categories([
        "base", "forecast"], ordered=True)
    # join.to_csv('nemos_join_base_forecast_antes.csv', index=False)
    join = join.drop_duplicates(['nemo'], keep='last')
    # join.to_csv('nemos_join_base_forecast_final.csv', index=False)
    # nemos_forecast.to_csv('nemos_in_forecast.csv', index=True)
    # nemos_restricciones.to_csv('nemos_sign.csv', index=True)

    # final_result = pd.merge(nemos_restricciones, nemos_forecast,
    #                         left_on='nemo', right_on='nemo', how='left')
    # final_result['sign'] = np.where((pd.isna(final_result.sign_y)) & (
    #     final_result.tecnologia == 'INGENIOS AZUCAREROS'), 2, final_result.sign_x)

    # final_result.to_csv('final_result.csv', index=True)
    result = map(lambda x: {"query_filter": {
        "name": x}, "sign": "<" if int(join.loc[(join.nemo == x), 'sign']) == 1 else ">"}, list(join.nemo.unique()))
    return list(result)


def get_json_volumen_cota():
    vol_cota = volumen_cota()
    nemos_cotas = vol_cota.nemo.unique()
    lista = ['AGU-H2', 'AGU-H3', 'CAN-H2', 'CHX-H2', 'CHX-H3', 'CHX-H4', 'CHX-H5', 'JUR-H2', 'JUR-H3', 'LES-H2', 'LVA-H2', 'LVA-H4', 'OX2-H2', 'OX2-H3',
             'OXE-H2', 'PNA-H2', 'PNA-H3', 'PVI-H2', 'RE2-H2', 'RE2-H3', 'RE2-H4', 'RE4-H2', 'REN-H2', 'REN-H3', 'STS-H2', 'VDA-H2', 'XAC-H2', 'XAD-H2']
    result = []
    for nemo in nemos_cotas:
        volxcota = vol_cota[(vol_cota.nemo == nemo)]
        if nemo not in lista:
            volxcota['cota'] = volxcota['cota'].astype(float)
            volxcota['volumen'] = volxcota['volumen'].round(4)
            volxcota.sort_values(by=['cota'], ascending=True, inplace=True)
            print(volxcota)
            query_filter = {"query_filter": {"name": nemo}, "Vmin": [
                volxcota.volumen.min()], "Vmax": [volxcota.volumen.max()]}
            contador = 1
            for index, row in volxcota.iterrows():
                print(row.cota)
                query_filter[f"SxH_Storage({contador})"] = row.volumen
                query_filter[f"SxH_Head({contador})"] = row.cota
                contador += 1
            result.append(query_filter)
    else:
        print("Este nemo no se tomo en cuenta", nemo)
    return result


def send_request(command, accion, data=None, host='localhost', port=8000):
    run_id = data.get('run_id') if 'run_id' in (
        data if type(data) is dict else {}).keys() else 0

    endpoints = {"list": f"http://{host}:{port}/listcases",
                 "copy": f"http://{host}:{port}/copycase",
                 "run": f"http://{host}:{port}/run",
                 "status": f"http://{host}:{port}/status?run_id={run_id}"}
    try:
        if command == 'post':
            if accion != 'status':
                data = json.dumps(data, indent=4)
                response = requests.post(url=endpoints.get(accion), data=data)
                return response
            else:
                print(endpoints.get(accion))
                response = requests.post(url=endpoints.get(accion))
                return response
        elif command == 'get':
            response = requests.post(url=endpoints.get(accion))
            return response
        else:
            return {'error': f'{command} is invalid'}
    except requests.exceptions.RequestException as error:
        response = {'status': 'unavailable', 'error': f'{error}'}
        return response


def update_case(CASE_DIR, CASE, FILE):
    NEW_CASE = f'{CASE}_C'
    data = {"casename": CASE,
            "newcasename": NEW_CASE,
            "updatefilename": FILE}
    copy = send_request('post', 'copy', data)
    copy = copy.json()
    time.sleep(4)
    print(copy)
    if copy.get('updatelog') == '':
        shutil.rmtree(os.path.join(CASE_DIR, CASE), ignore_errors=True)
        print("::CASO::COPIADO")
        status = run_case(CASE_DIR, NEW_CASE)
        # status = {'out':"only_copy"}
        print("Devolviendo el status", status)  # status copy
        return status, NEW_CASE  # status copy
    else:
        return {'status': 'error', 'error': [copy]}


def run_case(CASE_DIR, CASE):
    data = {"casename": CASE, "model": "ncp"}
    run = send_request('post', 'run', data)
    time.sleep(1)
    run = run.json()
    run_id = run.get("run_id")
    check = check_run(CASE_DIR, run_id, CASE)
    return check


def check_run(CASE_DIR, id, CASE):
    status = {"finished": np.nan, "model_error_code": np.nan, "out": np.nan}
    ROOT = r"C:\psrhttpfiles\runs"
    params = {'run_id': str(id)}
    NEW_RUN = CASE.replace("_C", "_R")
    ERROR = CASE.replace("_C", "_E")
    status_rename = True
    while np.isnan(status.get('finished')) and np.isnan(status.get('model_error_code')):
        listen = send_request('post', 'status', params)
        time.sleep(1)
        listen = listen.json()
        print(listen)
        if listen.get('finished') == 1 and listen.get('model_error_code') == 0 and listen.get('output') is not None:
            while np.isnan(status.get('finished')) and np.isnan(status.get('model_error_code')):
                listen = send_request('post', 'status', params)
                listen = listen.json()
                time.sleep(1)
                if listen.get('finished') == 1 and listen.get('model_error_code') == 0 and listen.get('output') is not None:
                    if 'Escribiendo los archivos de salida...' in listen.get('output'):
                        status['finished'] = listen.get('finished')
                        status['model_error_code'] = listen.get(
                            'model_error_code')
                        print(
                            f"::CASO FINALIZADO:: run_id:{params.get('run_id')}")
                        time.sleep(2.5)
                        status['out'] = "success"
                        print(CASE_DIR)
                        while (status_rename):
                            try:
                                os.rename(os.path.join(ROOT, params.get(
                                    'run_id')), os.path.join(ROOT, NEW_RUN))
                                status_rename = False
                            except OSError:
                                print("Error al renombrar, se intentara de nuevo")
                                status_rename = True
                elif listen.get('finished') == 2 and listen.get('model_error_code') == -1 and listen.get('error_message') is not None:
                    if 'A problem ocurred while running csvcnv' in listen.get('output'):
                        print(
                            f"::CASO NO PUDO SER CORRIDO FINALIZADO:: run_id:{params.get('run_id')}")
                        status['finished'] = listen.get('finished')
                        status['model_error_code'] = listen.get(
                            'model_error_code')
                        os.rename(os.path.join(ROOT, params.get(
                            'run_id')), os.path.join(ROOT, ERROR))

        elif listen.get('finished') == 0 and listen.get('model_error_code') == 0:
            status['finished'] = listen.get('finished')
            status['model_error_code'] = listen.get('model_error_code')
        elif listen.get('finished') == 0 and listen.get('model_error_code') == -1 and listen.get('output') is not None:
            if 'Hard Lock Not Found - ERROR CODE 0003' in listen.get('output'):
                winsound.Beep(freq, duration)
                win32api.MessageBox(
                    0, 'Conecte su dongle y vuelva a ejecutar el proceso', 'Alert')
                quit()
            if 'Thermal Ramping Constraints' in listen.get('output'):
                print(
                    f"::CASO NO PUDO SER CORRIDO POR RAMPING :: run_id:{params.get('run_id')}")
                status['finished'] = listen.get('finished')
                status['model_error_code'] = listen.get(
                    'model_error_code')
                os.rename(os.path.join(ROOT, params.get(
                    'run_id')), os.path.join(ROOT, ERROR))
                status['out'] = "Thermal Ramping Constraints"
            elif 'Mosel failed to run the model' in listen.get('output'):
                print(
                    f"::CASO NO PUDO SER CORRIDO POR RAMPING :: run_id:{params.get('run_id')}")
                status['finished'] = listen.get('finished')
                status['model_error_code'] = listen.get(
                    'model_error_code')
                os.rename(os.path.join(ROOT, params.get(
                    'run_id')), os.path.join(ROOT, ERROR))
                status['out'] = "Mosel failed to run the model"

        elif listen.get('finished') == 2 and listen.get('model_error_code') == -1 and listen.get('output') is None:
            if 'A problem ocurred while running csvcnv' in listen.get('error_message'):
                # winsound.Beep(freq, duration)
                # win32api.MessageBox(
                #     0, 'El caso tiene inconsistencias, revisar el sddpout', 'Error')
                status['finished'] = listen.get('finished')
                status['model_error_code'] = listen.get(
                    'model_error_code')
                os.rename(os.path.join(ROOT, params.get(
                    'run_id')), os.path.join(ROOT, ERROR))
                status['out'] = "A problem ocurred while running csvcnv"
    return status


def crea_dats(day, hours, path):
    days = hours / 24
    year, week_num, day_of_week = day.isocalendar()
    crear_caudcp(day, hours, path)
    time.sleep(1)
    crear_condih(path)
    time.sleep(1)
    crear_cprgergu(day, days, path)
    time.sleep(1)
    # crear_ccombuGU(path)
    # crear_cpfcstgu(day, days, path)
    time.sleep(1)
    crear_cpresgGU(day, days, path)  # reserva secundaria

    if day.weekday() == 6:
        number_of_week = int(day.strftime("%V")) + 1
    else:
        number_of_week = int(day.strftime("%V"))

    f_day = get_start_and_end_date_from_calendar_week(year, number_of_week, 1)
    # l_day = get_start_and_end_date_from_calendar_week(year, number_of_week, 2)
    if day_of_week == 6:
        crear_cppmhigu(f_day, 8, path)
        crear_cppmtrgu(f_day, 8, path)
    else:
        crear_cppmhigu(f_day, 7, path)
        crear_cppmtrgu(f_day, 7, path)
    return 'Exito'


def setup_ncp(day, hours: int, code: bool, **kw):
    """Realiza la llamada a todas las funciones a cargar al formato json

    :param day: dia inicial para la generacion de horas
    :type day: datetime
    :param hours: horas a generar con frecuencia de 1H hasta las 23H
    :type hours: int
    :param code: True retorna codigo y False nemo
    :type code: bool
    :return: retorna un diccionario con todos los datos a cargar para un caso estandar
    :rtype: dict
    """
    escenarios = pd.DataFrame()
    demanda = pd.DataFrame()
    # with concurrent.futures.ProcessPoolExecutor() as executor:
    confi = get_json_configuration_case(kw['config'])
    oferta_hidro = get_json_oferta_hidro(day, hours, code)
    restriccion, forecast, nemos_restri = get_json_restricciones_generacion(
        day, hours, 8, code)
    # restriccion, forecast, nemos_restri = restricciones_gen.result()
    constraint_type = get_json_constraint_type(nemos_restri, forecast)
    caudales,escenario_c = get_json_cauales_hidro(day, hours, code)
    d, cfs, e = get_json_demanda(day, hours, kw['tipo'], kw['pais'])
    # d, cfs, e = d.result()
    demanda = d
    escenarios = pd.concat([escenarios, e, escenario_c])
    precio_combustible = get_json_precios_combustible(day, hours, code)
    eolicas = get_json_eolicas(day, hours, code)

    condicion_inicial_ther = get_json_con_termicas(day, code)
    con_ini_hidros = get_json_con_hidros(day, code)
    generacion_meta = get_json_generacion_meta(day, hours)
    confi_ini_rampas = condicion_inicial_rampas(kw['diaria'], kw['pais'])
    cotas = get_json_cotas(day, hours, code)
    rampas_previo, rampa = get_json_rampas(code, forecast)
    # rampas_previo, rampa = r_rampas.result()
    config_thermals = get_json_config_termicas(kw['base'])
    marge_rro = get_json_margen_rro(day, hours, kw['tipo'])
    c_hydros, c_thermal = get_json_c_installed()
    vol_alerta = get_json_volumen_alerta(day, hours)

    # c_hydros, c_thermal = capacidad.result()
    c_installed_hydros = get_json_c_hydro(c_hydros)
    c_installed_thermal = get_json_c_thermal(c_thermal, forecast)
    mantosh, mantost = get_json_mantenimientos(day, 11, cfs, forecast)
    # mantosh, mantost = mantos.result()
    mantos_hydros = get_json_mantos_hydros(mantosh)
    mantos_thermals = get_json_mantos_thermal(mantost)
    termicas_activas = get_json_plantas_activas(day, 11, rampa, mantost)
    
    # RRO
    rro_hidros, rro_termicas = get_json_rro(day, hours)

    # volxcota = executor.submit(get_json_volumen_cota)

    process_continue = missing_inputs_alarm(day)
    input_verification_file = read_input_verification_file()
    if process_continue == False:
        print(input_verification_file)
        sys.exit("PROCESO ABORTADO. INPUTS NO DISPONIBLES.")

    write_input_verification_file(kw['case_dir']+"/", input_verification_file, kw["tipo"])

    setup_json = {}
    setup_json['PSRStudy'] = confi
    setup_json['PSRHydroPlant'] = oferta_hidro
    setup_json['PSRHydroPlant'] += cotas
    setup_json['PSRHydroPlant'] += con_ini_hidros
    setup_json['PSRHydroPlant'] += mantos_hydros
    setup_json['PSRHydroPlant'] += c_installed_hydros
    setup_json['PSRHydroPlant'] += vol_alerta
    setup_json['PSRHydroPlant'] += rro_hidros
    # setup_json['PSRHydroPlant'] += volxcota
    setup_json['PSRGenerationConstraintData'] = restriccion
    setup_json['PSRGenerationConstraintData'] += constraint_type
    setup_json['PSRGaugingStation'] = caudales
    setup_json['PSRDemandSegment'] = demanda
    setup_json['PSRFuel'] = precio_combustible
    setup_json['PSRGndPlant'] = eolicas
    setup_json['PSRThermalPlant'] = condicion_inicial_ther
    setup_json['PSRThermalPlant'] += rampas_previo
    setup_json['PSRThermalPlant'] += config_thermals
    setup_json['PSRThermalPlant'] += mantos_thermals
    setup_json['PSRThermalPlant'] += c_installed_thermal
    setup_json['PSRThermalPlant'] += termicas_activas
    setup_json['PSRThermalPlant'] += rro_termicas
    setup_json['PSRTargetGeneration'] = generacion_meta
    setup_json['PSRSystem'] = confi_ini_rampas
    setup_json['PSRReserveGenerationConstraintData'] = marge_rro

    return setup_json, escenarios


def execute_case(day, demanda):
    # if exists("input_verification.csv"):
    #     os.remove("input_verification.csv")
    day = day.to_pydatetime()
    response = send_request('get', 'list')
    current = datetime.now()
    print("Current date:", str(current))
    tipo = demanda
    print(f"La demanda es {tipo}")
    logging.basicConfig(filename='NCP.log',
                        encoding='utf-8', level=logging.CRITICAL)
    pais = 'GUATEMALA'
    code = False
    print("La fecha del caso es ", day)
    hours = 48
    days = (hours // 24)
    diaria = True
    idioma = "ES"
    idioma = 1 if idioma == "ES" else 0  # 0 es ingles
    BASE_DIR = Path(os.path.abspath(os.getcwd()), "BASE_CASE")
    CASES_DIR = Path("C:/", "psrhttpfiles", "cases")
    RUNS_DIR = "C:/psrhttpfiles/runs/"
    name_json = 'setup_ncp.json'
    config = {'config': {"fechai": day,
                         "idioma": idioma, 'horai': 0, 'horaf': hours}}

    today = datetime.today().strftime('%d%b%Y_%H_%M_%S').upper()
    id_case = f"PDD_V26_F_{day.strftime('%d%b%Y').upper()}_FA_{today}_{tipo.upper()}"

    response = response.json()
    NEW_CASE = Path("C:/", "psrhttpfiles", "cases", id_case)
    JSON_DIR = Path("C:/", "psrhttpfiles",
                    "cases", id_case, name_json)
    if id_case not in response.get('cases'):
        try:
            shutil.copytree(BASE_DIR, NEW_CASE)
            time.sleep(1)
            crea_dats(day, hours, NEW_CASE)
        except shutil.Error as exc:
            print(exc)
        setup, escenarios = setup_ncp(day, hours, code, tipo=tipo,
                                      pais=pais, diaria=diaria, config=config, base=NEW_CASE, case_dir=f"C:/psrhttpfiles/cases/{id_case}")
        jsonString = json.dumps(setup, indent=4)
        escenarios = pd.concat([escenarios, pd.DataFrame({"tipo_input": ['PAIS', 'DZ'], "valor_input": [
            'GTM', '1'], "fuente_input": ['NA', 'NA'], "version_input": ['NA', '1']})], ignore_index=False)
        print("::ESCENARIOS:: \n", escenarios)
        with open(JSON_DIR, "w") as outfile:
            outfile.write(jsonString)
        status, case_run = update_case(
            CASES_DIR, id_case, name_json)
        print(case_run)
        id_run = case_run.replace("_C", "_R")

        time.sleep(2)
        if status['out'] == 'success':
            ROOT = (RUNS_DIR + id_run + "/")
            ROOT = os.path.join(
                os.path.dirname(ROOT), 'defcitcp.csv')
            deficit = pd.read_csv(ROOT)

            deficit = deficit.iloc[2: len(deficit)]
            deficit.columns = deficit.iloc[0].str.strip()
            deficit = deficit.drop(deficit.index[0])
            deficit = deficit[[
                'Estg', 'Ser.', 'Pat.', 'GUATEMALA']]
            deficit.reset_index()
            deficit['GUATEMALA'] = deficit['GUATEMALA'].astype(
                float)
            if (deficit.GUATEMALA > 0).any():
                print(":::REVISE EL CASO TIENE DEFICIT")
            else:
                print(":::EL CASO NO TIENE DEFICIT")
            POST_NCP_PROCESS(day, days, escenarios,
                             (RUNS_DIR + id_run), False)
        current = datetime.now()
        print("Current date:", str(current))
        winsound.Beep(freq, duration)
    else:
        print('caso_duplicado')


def main():
    """
    Realiza la llamada a setup_ncp
    """

    response = send_request('get', 'list')

    if (type(response) is dict) and ('status' in response.keys()) and (response.get('status') == 'unavailable'):
        print("check availability api")
        os.startfile(r"C:\PSRHttpApi\PSRHttpApi.exe")
        main()
    else:
        rangos_generar = pd.date_range(
            start=datetime.today().date(), end=datetime.today().date(), freq='D')
        # rangos_generar = pd.date_range(
        #     start="2023-02-18", end="2023-02-19", freq='D')
        for day in rangos_generar:
            demandas = ['yhat_wrf', 'yhat_lower_wrf','yhat_upper_wrf']  #,  
            Parallel(n_jobs=2)(delayed(execute_case)(day, demanda)
                               for demanda in demandas)
        send_final_results(day.date())
        if exists("input_verification.csv"):
            os.remove("input_verification.csv")
        time.sleep(5)
        subprocess.call("TASKKILL /F /IM PSRHttpApi.exe", shell=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        day = (datetime.now()).strftime("%d/%m/%Y")
        message_error = str(traceback.format_exc())
        send_notification(message=message_error)
        send_message_teams(day=day, status="error",
                           api=GTM_API_TEAMS, error=message_error)

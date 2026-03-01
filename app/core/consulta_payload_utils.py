from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


CURP_CHARSET = "0123456789ABCDEFGHIJKLMNÑOPQRSTUVWXYZ"
CURP_MAP = {char: idx for idx, char in enumerate(CURP_CHARSET)}


def parse_int_from_text(value: Optional[str]) -> int:
    if not value:
        return 0
    match = re.search(r"\d+", str(value))
    return int(match.group()) if match else 0


def calcular_digito_verificador_curp(curp17: str) -> str:
    suma = 0
    for idx, char in enumerate(curp17):
        valor = CURP_MAP.get(char, 0)
        suma += valor * (18 - idx)
    digito = (10 - (suma % 10)) % 10
    return str(digito)


def normalize_form_data(form_data: Dict[str, Any], *, required_sentinels: Optional[set[str]] = None) -> Dict[str, Any]:
    sentinels = required_sentinels or set()
    cleaned: Dict[str, Any] = {}
    for key, value in form_data.items():
        if isinstance(value, str):
            val = value.strip()
            if val == "":
                cleaned[key] = None
            else:
                upper = val.upper()
                cleaned[key] = upper if upper in sentinels else val
        else:
            cleaned[key] = value
    return cleaned


def apply_aliases(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    if data_dict.get("vejiga_coagulos_tipo") and not data_dict.get("vejiga_hematuria_coagulos"):
        data_dict["vejiga_hematuria_coagulos"] = data_dict.get("vejiga_coagulos_tipo")
    if data_dict.get("vejiga_hematuria_coagulos") and not data_dict.get("vejiga_coagulos_tipo"):
        data_dict["vejiga_coagulos_tipo"] = data_dict.get("vejiga_hematuria_coagulos")
    return data_dict


def calcular_indice_tabaquico(cigarros_dia: Optional[int], anios_fumando: Optional[int]) -> Optional[str]:
    if cigarros_dia and anios_fumando and cigarros_dia > 0 and anios_fumando > 0:
        indice = (cigarros_dia * anios_fumando) / 20
        return f"{indice:.1f} pq/año"
    return None


def calcular_scores_litiasis(tamano: Optional[float], loc: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not tamano or not loc:
        return None, None
    guys = "I"
    if loc == "coraliforme":
        guys = "IV"
    elif loc == "renal_inf":
        guys = "III"
    elif tamano > 20:
        guys = "II"

    base_croes = 250 - (tamano * 1.5)
    if loc == "coraliforme":
        base_croes -= 80
    if base_croes < 0:
        base_croes = 0
    croes = f"{int(round(base_croes))} (Est)"
    return f"Grado {guys}", croes


def generar_nota_soap(data_dict: Dict[str, Any]) -> Dict[str, str]:
    nombre = data_dict.get("nombre", "Paciente")
    diagnostico = data_dict.get("diagnostico_principal", "diagnóstico no especificado")
    imc = data_dict.get("imc", "N/E")
    plan = data_dict.get("plan_especifico", "Plan pendiente de definir")
    ta = data_dict.get("ta", "N/E")
    fc = data_dict.get("fc", "N/E")
    temp = data_dict.get("temp", "N/E")
    indice_tabaquico = data_dict.get("indice_tabaquico", "N/E")
    padecimiento = data_dict.get("padecimiento_actual", "Sin síntomas referidos.")

    subjetivo = f"{nombre} refiere: {padecimiento}"
    objetivo = f"TA {ta}, FC {fc}, Temp {temp}. IMC {imc}. Índice tabáquico {indice_tabaquico}."
    analisis = f"Cuadro compatible con {diagnostico}. Evolución clínica en seguimiento."
    plan_text = f"{plan}. Control y seguimiento según protocolo."
    return {
        "subjetivo": subjetivo,
        "objetivo": objetivo,
        "analisis": analisis,
        "plan": plan_text,
    }


def detectar_inconsistencias(data_dict: Dict[str, Any]) -> List[str]:
    inconsistencias: List[str] = []
    edad = data_dict.get("edad")
    sexo = data_dict.get("sexo")
    diag = data_dict.get("diagnostico_principal") or ""

    male_only = {"ca_prostata", "hpb", "tumor_incierto_prostata"}
    if sexo and str(sexo).lower().startswith("fem") and diag in male_only:
        inconsistencias.append("Diagnóstico típicamente masculino en paciente femenino. Verificar sexo/diagnóstico.")

    if edad is not None and isinstance(edad, (int, float)) and str(diag).startswith("ca_") and edad < 18:
        inconsistencias.append("Paciente menor de edad con diagnóstico oncológico. Verificar edad y diagnóstico.")

    if edad is not None and isinstance(edad, (int, float)) and diag == "ca_prostata" and edad < 30:
        inconsistencias.append("Paciente joven con cáncer de próstata. Verificar edad y diagnóstico.")

    imc = data_dict.get("imc")
    if isinstance(imc, (int, float)) and (imc < 10 or imc > 60):
        inconsistencias.append("IMC fuera de rangos esperados. Verificar peso/talla.")

    return inconsistencias


def aplicar_derivaciones(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    if not data_dict.get("indice_tabaquico"):
        it = calcular_indice_tabaquico(data_dict.get("cigarros_dia"), data_dict.get("anios_fumando"))
        if it:
            data_dict["indice_tabaquico"] = it

    if not data_dict.get("lit_guys_score") or not data_dict.get("lit_croes_score"):
        guys, croes = calcular_scores_litiasis(data_dict.get("lit_tamano"), data_dict.get("lit_localizacion"))
        if guys and not data_dict.get("lit_guys_score"):
            data_dict["lit_guys_score"] = guys
        if croes and not data_dict.get("lit_croes_score"):
            data_dict["lit_croes_score"] = croes

    subsecuente_fields = [
        "subsecuente_subjetivo",
        "subsecuente_objetivo",
        "subsecuente_analisis",
        "subsecuente_plan",
    ]
    if not any(data_dict.get(field) for field in subsecuente_fields):
        nota = generar_nota_soap(data_dict)
        data_dict.setdefault("subsecuente_subjetivo", nota["subjetivo"])
        data_dict.setdefault("subsecuente_objetivo", nota["objetivo"])
        data_dict.setdefault("subsecuente_analisis", nota["analisis"])
        data_dict.setdefault("subsecuente_plan", nota["plan"])

    return data_dict

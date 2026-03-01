#!/usr/bin/env python3
"""
Carga aditiva de datos sintéticos de pacientes para validación funcional.

Objetivo:
- Insertar 100 pacientes en enero 2026.
- Poblar tablas clínicas/quirúrgicas relacionadas para estresar reportes.
- No modifica rutas ni lógica; solo agrega registros de prueba.
"""

from __future__ import annotations

import argparse
import random
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Tuple

from sqlalchemy import create_engine, text


CLINICAL_URL = "postgresql+psycopg2://Faudes:1234@localhost:5432/urologia"
SURGICAL_URL = "postgresql+psycopg2://Faudes:1234@localhost:5432/urologia_quirurgico"
SEED_TAG = "SIM_ENE_2026"


NOMBRES = [
    ("GARCIA", "LOPEZ", "MIGUEL"),
    ("HERNANDEZ", "RAMIREZ", "CARLOS"),
    ("MARTINEZ", "PEREZ", "JORGE"),
    ("RODRIGUEZ", "FLORES", "ALBERTO"),
    ("SANCHEZ", "TORRES", "DAVID"),
    ("GOMEZ", "DIAZ", "MARIA"),
    ("PEREZ", "MORALES", "ANA"),
    ("LOPEZ", "VARGAS", "LUISA"),
    ("RAMIREZ", "CASTRO", "ELENA"),
    ("TORRES", "RUIZ", "PAOLA"),
]


DIAGNOSTICOS: List[Tuple[str, str]] = [
    ("CANCER DE PROSTATA", "ONCOLOGICO"),
    ("CANCER RENAL", "ONCOLOGICO"),
    ("CANCER DE VEJIGA", "ONCOLOGICO"),
    ("CANCER DE TESTICULO", "ONCOLOGICO"),
    ("TUMOR SUPRARRENAL", "ONCOLOGICO"),
    ("CALCULO DEL RIÑON", "LITIASIS_URINARIA"),
    ("CALCULO DEL URETER", "LITIASIS_URINARIA"),
    ("CALCULO DE LA VEJIGA", "LITIASIS_URINARIA"),
    ("CRECIMIENTO PROSTATICO OBSTRUCTIVO", "FUNCIONAL"),
    ("PIELONEFRITIS", "INFECCIOSO"),
    ("ABSCESO RENAL", "INFECCIOSO"),
]

PROCEDIMIENTOS = [
    "NEFRECTOMIA RADICAL",
    "NEFRECTOMIA SIMPLE",
    "NEFROLITOTRICIA LASER FLEXIBLE",
    "URETEROLITOTRICIA LASER FLEXIBLE",
    "CISTOLITOTRICIA",
    "PROSTATECTOMIA RADICAL",
    "RESECCION TRANSURETRAL DE VEJIGA",
    "CISTOSTOMIA",
    "ECIRS",
]

ABORDAJES = ["ABIERTO", "LAPAROSCOPICO", "ENDOSCOPICO", "PERCUTANEA"]
HGZS = ["HGZ 27", "HGZ 1A", "HGR 2", "HGZ 53", "UMAE RAZA"]
CIRUJANOS = ["DR. PEREZ", "DR. TORRES", "DRA. MARTINEZ", "DR. LOPEZ", "DR. GARCIA"]
SERVICIOS = ["UROLOGIA", "MEDICINA INTERNA", "NEFROLOGIA"]


def random_january_date(rng: random.Random) -> date:
    return date(2026, 1, 1) + timedelta(days=rng.randint(0, 30))


def mk_curp(seed_idx: int, sexo: str, rng: random.Random) -> str:
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    pref = "".join(rng.choice(letters) for _ in range(4))
    yy = "26"
    mm = f"{rng.randint(1,12):02d}"
    dd = f"{rng.randint(1,28):02d}"
    sex = "H" if sexo == "MASCULINO" else "M"
    ent = "DF"
    tail = f"{seed_idx%10}{rng.choice(letters)}{rng.randint(0,9)}"
    return f"{pref}{yy}{mm}{dd}{sex}{ent}{tail}"


def mk_nss(seed_idx: int) -> str:
    # 11 dígitos, prefijo reservado para simulación.
    return f"99{260000000 + seed_idx:09d}"[:11]


def build_nombre(seed_idx: int, sexo: str) -> str:
    ape1, ape2, nom = NOMBRES[seed_idx % len(NOMBRES)]
    if sexo == "FEMENINO" and nom in {"MIGUEL", "CARLOS", "JORGE", "ALBERTO", "DAVID"}:
        nom = "MARIA"
    if sexo == "MASCULINO" and nom in {"MARIA", "ANA", "LUISA", "ELENA", "PAOLA"}:
        nom = "JOSE"
    return f"{ape1} {ape2} {nom} {SEED_TAG}"


def run_seed(total: int, seed: int, dry_run: bool = False) -> Dict[str, int]:
    rng = random.Random(seed)
    clinical_engine = create_engine(CLINICAL_URL)
    surgical_engine = create_engine(SURGICAL_URL)

    counters = Counter()
    diag_counter = Counter()
    estatus_counter = Counter()
    urg_counter = Counter()
    sex_counter = Counter()
    by_day = defaultdict(int)

    with clinical_engine.begin() as cconn, surgical_engine.begin() as sconn:
        for i in range(1, total + 1):
            sexo = "MASCULINO" if rng.random() < 0.7 else "FEMENINO"
            edad = rng.randint(18, 84)
            fecha_registro = random_january_date(rng)
            diag, grupo_pat = rng.choice(DIAGNOSTICOS)
            proc = rng.choice(PROCEDIMIENTOS)
            abordaje = rng.choice(ABORDAJES)
            hgz = rng.choice(HGZS)
            nombre = build_nombre(i, sexo)
            nss = mk_nss(i)
            curp = mk_curp(i, sexo, rng)

            if dry_run:
                consulta_id = i
            else:
                consulta_id = cconn.execute(
                    text(
                        """
                        INSERT INTO consultas (
                          fecha_registro, curp, nss, agregado_medico, nombre, edad, sexo,
                          diagnostico_principal, estatus_protocolo, plan_especifico, evento_clinico
                        ) VALUES (
                          :fecha_registro, :curp, :nss, :agregado, :nombre, :edad, :sexo,
                          :diag, :estatus_protocolo, :plan, :evento
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "fecha_registro": fecha_registro,
                        "curp": curp,
                        "nss": nss,
                        "agregado": SEED_TAG,
                        "nombre": nombre,
                        "edad": edad,
                        "sexo": sexo,
                        "diag": diag,
                        "estatus_protocolo": "completo" if rng.random() < 0.75 else "incompleto",
                        "plan": "Seguimiento y resolución quirúrgica",
                        "evento": SEED_TAG,
                    },
                ).scalar_one()

            counters["consultas"] += 1
            sex_counter[sexo] += 1
            diag_counter[diag] += 1
            by_day[fecha_registro.isoformat()] += 1

            # Hospitalización (~58%)
            if rng.random() < 0.58:
                dias_est = rng.randint(1, 12)
                fecha_egreso = fecha_registro + timedelta(days=dias_est)
                if not dry_run:
                    cconn.execute(
                        text(
                            """
                            INSERT INTO hospitalizaciones (
                              consulta_id, fecha_ingreso, fecha_egreso, motivo, servicio, cama, estatus,
                              nss, agregado_medico, nombre_completo, edad, sexo, diagnostico, hgz_envio,
                              estatus_detalle, dias_hospitalizacion, ingreso_tipo, estado_clinico, uci, observaciones
                            ) VALUES (
                              :consulta_id, :fi, :fe, :motivo, :servicio, :cama, :estatus,
                              :nss, :agregado, :nombre, :edad, :sexo, :diag, :hgz,
                              :estatus_detalle, :dias, :ingreso_tipo, :estado_clinico, :uci, :obs
                            )
                            """
                        ),
                        {
                            "consulta_id": consulta_id,
                            "fi": fecha_registro,
                            "fe": fecha_egreso,
                            "motivo": diag,
                            "servicio": rng.choice(SERVICIOS),
                            "cama": f"{rng.randint(101, 420)}-{rng.choice('ABC')}",
                            "estatus": "EGRESADO" if fecha_egreso <= date(2026, 1, 31) else "ACTIVO",
                            "nss": nss,
                            "agregado": SEED_TAG,
                            "nombre": nombre,
                            "edad": edad,
                            "sexo": sexo,
                            "diag": diag,
                            "hgz": hgz,
                            "estatus_detalle": "ESTABLE",
                            "dias": dias_est,
                            "ingreso_tipo": "URGENCIA" if rng.random() < 0.4 else "PROGRAMADO",
                            "estado_clinico": rng.choice(["ESTABLE", "DELICADO", "GRAVE"]),
                            "uci": "SI" if rng.random() < 0.08 else "NO",
                            "obs": SEED_TAG,
                        },
                    )
                    # Vitals/labs de ejemplo
                    ts = datetime.combine(fecha_registro, time(hour=rng.randint(6, 20), minute=0))
                    cconn.execute(
                        text(
                            """
                            INSERT INTO vitals (consulta_id, patient_id, timestamp, hr, sbp, dbp, temp, peso, talla, imc, source)
                            VALUES (:cid, :pid, :ts, :hr, :sbp, :dbp, :temp, :peso, :talla, :imc, :src)
                            """
                        ),
                        {
                            "cid": consulta_id,
                            "pid": nss,
                            "ts": ts,
                            "hr": rng.randint(60, 120),
                            "sbp": rng.randint(95, 165),
                            "dbp": rng.randint(55, 105),
                            "temp": round(rng.uniform(36.0, 38.8), 1),
                            "peso": round(rng.uniform(52, 96), 1),
                            "talla": round(rng.uniform(1.48, 1.86), 2),
                            "imc": round(rng.uniform(20.0, 33.5), 1),
                            "src": SEED_TAG,
                        },
                    )
                    for code, name, unit, lo, hi in [
                        ("CREAT", "Creatinina", "mg/dL", 0.7, 2.5),
                        ("HB", "Hemoglobina", "g/dL", 7.2, 15.9),
                        ("LEU", "Leucocitos", "/uL", 4000, 18500),
                        ("PLT", "Plaquetas", "/uL", 75000, 420000),
                        ("NA", "Sodio", "mmol/L", 125, 149),
                        ("K", "Potasio", "mmol/L", 2.8, 5.9),
                    ]:
                        value = round(rng.uniform(lo, hi), 2)
                        cconn.execute(
                            text(
                                """
                                INSERT INTO labs (consulta_id, patient_id, timestamp, test_code, test_name, value, unit, source)
                                VALUES (:cid, :pid, :ts, :code, :name, :value, :unit, :src)
                                """
                            ),
                            {
                                "cid": consulta_id,
                                "pid": nss,
                                "ts": ts,
                                "code": code,
                                "name": name,
                                "value": str(value),
                                "unit": unit,
                                "src": SEED_TAG,
                            },
                        )
                counters["hospitalizaciones"] += 1

            # Programación quirúrgica (~72%)
            if rng.random() < 0.72:
                fecha_prog = random_january_date(rng)
                estatus = rng.choices(
                    ["PROGRAMADA", "REALIZADA", "CANCELADA"],
                    weights=[0.5, 0.38, 0.12],
                    k=1,
                )[0]
                fecha_real = None
                if estatus == "REALIZADA":
                    fecha_real = min(fecha_prog + timedelta(days=rng.randint(0, 2)), date(2026, 1, 31))

                # registro clínico quirofano
                if dry_run:
                    qx_id = i
                else:
                    qx_id = cconn.execute(
                        text(
                            """
                            INSERT INTO quirofanos (
                              consulta_id, fecha_programada, fecha_realizacion, procedimiento, cirujano,
                              anestesiologo, quirofano, estatus, notas
                            ) VALUES (
                              :cid, :fp, :fr, :proc, :cir, :anes, :qx, :est, :notas
                            ) RETURNING id
                            """
                        ),
                        {
                            "cid": consulta_id,
                            "fp": fecha_prog,
                            "fr": fecha_real,
                            "proc": proc,
                            "cir": rng.choice(CIRUJANOS),
                            "anes": "DRA. ANESTESIA",
                            "qx": f"QX-{rng.randint(1, 8)}",
                            "est": estatus,
                            "notas": SEED_TAG,
                        },
                    ).scalar_one()

                if not dry_run:
                    sp_id = sconn.execute(
                        text(
                            """
                            INSERT INTO surgical_programaciones (
                              quirofano_id, consulta_id, curp, nss, agregado_medico, paciente_nombre, edad, sexo,
                              diagnostico_principal, patologia, grupo_patologia, procedimiento, procedimiento_programado,
                              grupo_procedimiento, abordaje, insumos_solicitados, requiere_intermed, hgz,
                              fecha_programada, fecha_realizacion, estatus, modulo_origen, creado_en, actualizado_en,
                              protocolo_completo, pendiente_programar, cirujano, sangrado_ml, transfusion
                            ) VALUES (
                              :qxid, :cid, :curp, :nss, :agregado, :nombre, :edad, :sexo,
                              :diag, :pat, :gpat, :proc, :proc,
                              :gproc, :abordaje, :insumos, :intermed, :hgz,
                              :fprog, :freal, :estatus, :origen, :creado, :actualizado,
                              :pc, :pp, :cirujano, :sangrado, :transfusion
                            ) RETURNING id
                            """
                        ),
                        {
                            "qxid": qx_id,
                            "cid": consulta_id,
                            "curp": curp,
                            "nss": nss,
                            "agregado": SEED_TAG,
                            "nombre": nombre,
                            "edad": edad,
                            "sexo": sexo,
                            "diag": diag,
                            "pat": diag,
                            "gpat": grupo_pat,
                            "proc": proc,
                            "gproc": abordaje,
                            "abordaje": abordaje,
                            "insumos": "ENDOURO (INTERMED)" if "LITOTRICIA" in proc else "EQUIPO DE CIRUGIA ABIERTA",
                            "intermed": "SI" if ("INTERMED" in ("ENDOURO (INTERMED)" if "LITOTRICIA" in proc else "")) else "NO",
                            "hgz": hgz,
                            "fprog": fecha_prog,
                            "freal": fecha_real,
                            "estatus": estatus,
                            "origen": "SIMULACION_ENE_2026",
                            "creado": datetime.utcnow(),
                            "actualizado": datetime.utcnow(),
                            "pc": "SI" if rng.random() < 0.8 else "NO",
                            "pp": "SI" if estatus == "PROGRAMADA" and rng.random() < 0.2 else "NO",
                            "cirujano": rng.choice(CIRUJANOS),
                            "sangrado": round(rng.uniform(20, 650), 1) if estatus == "REALIZADA" else None,
                            "transfusion": "SI" if estatus == "REALIZADA" and rng.random() < 0.18 else "NO",
                        },
                    ).scalar_one()

                    if estatus == "REALIZADA":
                        sconn.execute(
                            text(
                                """
                                INSERT INTO surgical_postquirurgicas (
                                  surgical_programacion_id, quirofano_id, consulta_id, fecha_realizacion, cirujano, sangrado_ml,
                                  diagnostico_postop, procedimiento_realizado, complicaciones, nota_postquirurgica,
                                  creado_en, actualizado_en, tiempo_quirurgico_min, transfusion, clavien_dindo, cateter_jj_colocado
                                ) VALUES (
                                  :spid, :qxid, :cid, :fr, :cir, :sangrado,
                                  :diag, :proc, :comp, :nota,
                                  :creado, :actualizado, :tq, :transfusion, :clavien, :jj
                                )
                                """
                            ),
                            {
                                "spid": sp_id,
                                "qxid": qx_id,
                                "cid": consulta_id,
                                "fr": fecha_real,
                                "cir": rng.choice(CIRUJANOS),
                                "sangrado": round(rng.uniform(30, 700), 1),
                                "diag": diag,
                                "proc": proc,
                                "comp": "SIN COMPLICACIONES" if rng.random() < 0.85 else "FIEBRE",
                                "nota": f"NOTA DE PRUEBA {SEED_TAG}",
                                "creado": datetime.utcnow(),
                                "actualizado": datetime.utcnow(),
                                "tq": rng.randint(45, 240),
                                "transfusion": "SI" if rng.random() < 0.15 else "NO",
                                "clavien": rng.choice(["I", "II", "IIIA"]),
                                "jj": "SI" if "LITOTRICIA" in proc and rng.random() < 0.55 else "NO",
                            },
                        )
                        counters["postquirurgicas"] += 1

                counters["programaciones"] += 1
                estatus_counter[estatus] += 1

                # Urgencias quirúrgicas (~30% de programaciones)
                if rng.random() < 0.3:
                    urg_estatus = "REALIZADA" if estatus == "REALIZADA" else "PROGRAMADA"
                    if not dry_run:
                        sconn.execute(
                            text(
                                """
                                INSERT INTO surgical_urgencias_programaciones (
                                  consulta_id, surgical_programacion_id, curp, nss, agregado_medico, paciente_nombre, edad, sexo,
                                  patologia, patologia_cie10, grupo_patologia, procedimiento_programado, grupo_procedimiento, abordaje,
                                  insumos_solicitados, requiere_intermed, hgz, ecog, charlson, fecha_urgencia, fecha_realizacion,
                                  estatus, cirujano, sangrado_ml, transfusion, modulo_origen, creado_en, actualizado_en
                                ) VALUES (
                                  :cid, NULL, :curp, :nss, :agregado, :nombre, :edad, :sexo,
                                  :pat, :cie10, :gpat, :proc, :gproc, :abordaje,
                                  :insumos, :intermed, :hgz, :ecog, :charlson, :fu, :fr,
                                  :estatus, :cir, :sangrado, :transfusion, :origen, :creado, :actualizado
                                )
                                """
                            ),
                            {
                                "cid": consulta_id,
                                "curp": curp,
                                "nss": nss,
                                "agregado": SEED_TAG,
                                "nombre": nombre,
                                "edad": edad,
                                "sexo": sexo,
                                "pat": diag,
                                "cie10": "N20.0" if "CALCULO DEL RIÑON" in diag else "C61" if "PROSTATA" in diag else "N13.2",
                                "gpat": grupo_pat,
                                "proc": proc,
                                "gproc": abordaje,
                                "abordaje": abordaje,
                                "insumos": "ENDOURO (INTERMED)" if "LITOTRICIA" in proc else "EQUIPO DE CIRUGIA ABIERTA",
                                "intermed": "SI" if "LITOTRICIA" in proc else "NO",
                                "hgz": hgz,
                                "ecog": rng.choice(["0", "1", "2"]),
                                "charlson": str(rng.randint(1, 6)),
                                "fu": fecha_prog,
                                "fr": fecha_real,
                                "estatus": urg_estatus,
                                "cir": rng.choice(CIRUJANOS),
                                "sangrado": round(rng.uniform(20, 550), 1) if urg_estatus == "REALIZADA" else None,
                                "transfusion": "SI" if urg_estatus == "REALIZADA" and rng.random() < 0.15 else "NO",
                                "origen": "SIMULACION_ENE_2026",
                                "creado": datetime.utcnow(),
                                "actualizado": datetime.utcnow(),
                            },
                        )
                    counters["urgencias_programaciones"] += 1
                    urg_counter[urg_estatus] += 1

    print("=== CARGA SINTETICA COMPLETADA ===")
    print(f"Tag simulacion: {SEED_TAG}")
    print(f"Consultas insertadas: {counters['consultas']}")
    print(f"Hospitalizaciones insertadas: {counters['hospitalizaciones']}")
    print(f"Programaciones quirurgicas insertadas: {counters['programaciones']}")
    print(f"Postquirurgicas insertadas: {counters['postquirurgicas']}")
    print(f"Urgencias quirurgicas insertadas: {counters['urgencias_programaciones']}")
    print("\n-- Desglose por sexo --")
    for k, v in sex_counter.most_common():
        print(f"{k}: {v}")
    print("\n-- Top diagnosticos --")
    for k, v in diag_counter.most_common(8):
        print(f"{k}: {v}")
    print("\n-- Estatus programacion --")
    for k, v in estatus_counter.items():
        print(f"{k}: {v}")
    print("\n-- Urgencias (estatus) --")
    for k, v in urg_counter.items():
        print(f"{k}: {v}")
    print("\n-- Capturas por dia (enero 2026) --")
    for d in sorted(by_day.keys()):
        print(f"{d}: {by_day[d]}")
    return dict(counters)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed sintético enero 2026")
    parser.add_argument("--total", type=int, default=100, help="Número de pacientes")
    parser.add_argument("--seed", type=int, default=2601, help="Semilla aleatoria")
    parser.add_argument("--dry-run", action="store_true", help="No inserta, solo simula")
    args = parser.parse_args()
    run_seed(total=args.total, seed=args.seed, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

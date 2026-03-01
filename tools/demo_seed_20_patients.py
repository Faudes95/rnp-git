#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple

import main


RND = random.Random()


def _make_curp(idx: int, born: date, sexo: str) -> str:
    sex_code = "H" if sexo.upper() == "MASCULINO" else "M"
    first = "DEMO"
    state = "DF"
    consonants = "RNP"
    check = f"{idx % 100:02d}"
    return f"{first}{born:%y%m%d}{sex_code}{state}{consonants}{check}"[:18]


def _rand_name(idx: int, run_tag: str) -> str:
    nombres = [
        "JUAN", "PEDRO", "CARLOS", "LUIS", "MIGUEL", "ANA", "MARIA", "LUISA", "PAULA", "ELENA",
        "ROBERTO", "JORGE", "SOFIA", "ANDREA", "DANIEL", "ADRIAN", "DAVID", "SARA", "LAURA", "RICARDO",
    ]
    apellidos1 = [
        "HERNANDEZ", "MARTINEZ", "GARCIA", "LOPEZ", "PEREZ", "SANCHEZ", "RAMIREZ", "CRUZ", "FLORES", "GOMEZ",
    ]
    apellidos2 = [
        "TORRES", "RIVERA", "ORTIZ", "CASTRO", "VARGAS", "MORALES", "REYES", "ROMERO", "MENDOZA", "SALINAS",
    ]
    return f"{apellidos1[idx % len(apellidos1)]} {apellidos2[(idx * 3) % len(apellidos2)]} {nombres[idx % len(nombres)]} DEMO{run_tag}"


def _safe_upper(v: Any) -> str:
    return str(v or "").strip().upper()


def seed_demo_patients(count: int = 20, seed: int | None = None) -> Dict[str, Any]:
    if seed is None:
        seed = int(datetime.now().strftime("%H%M%S"))
    RND.seed(seed)
    run_tag = datetime.now().strftime("%m%d")
    today = date.today()

    db = main.SessionLocal()
    sdb = main._new_surgical_session(enable_dual_write=True)
    created_consultas: List[main.ConsultaDB] = []
    created_programadas = 0
    created_urgencias = 0
    created_postqx = 0

    try:
        # 1) Crear 20 consultas demo.
        base_seed = seed % 1000
        for i in range(count):
            sexo = "MASCULINO" if i % 2 == 0 else "FEMENINO"
            edad = RND.randint(19, 83)
            born = today - timedelta(days=edad * 365 + RND.randint(0, 300))
            nss = f"98{base_seed:03d}{i:06d}"[:11]
            diag = RND.choice(main.QUIROFANO_PATOLOGIAS)
            consulta = main.ConsultaDB(
                fecha_registro=today - timedelta(days=RND.randint(1, 50)),
                curp=_make_curp(i, born, sexo),
                nss=nss,
                agregado_medico=f"DR. DEMO {1 + (i % 6)}",
                nombre=_rand_name(i, run_tag),
                fecha_nacimiento=born,
                edad=edad,
                sexo=sexo,
                diagnostico_principal=diag,
                estatus_protocolo="completo" if i % 3 != 0 else "incompleto",
                plan_especifico="PLAN DEMO",
                telefono=f"55{RND.randint(10000000, 99999999)}",
                email=f"demo{i}@rnp.local",
                alcaldia=RND.choice(["GUSTAVO A MADERO", "AZCAPOTZALCO", "CUAUHTEMOC", "MIGUEL HIDALGO"]),
                colonia=RND.choice(["LINDAVISTA", "VALLEJO", "ROMA NORTE", "POLANCO"]),
                cp=str(RND.randint(10000, 79999)),
            )
            db.add(consulta)
            created_consultas.append(consulta)

        db.commit()
        for c in created_consultas:
            db.refresh(c)

        # 2) Hospitalización + laboratorios para todos.
        for i, c in enumerate(created_consultas):
            ingreso = today - timedelta(days=RND.randint(0, 20))
            if i % 4 == 0:
                egreso = ingreso + timedelta(days=RND.randint(1, 5))
                if egreso > today:
                    egreso = None
            else:
                egreso = None
            ingreso_tipo = "URGENCIA" if i % 5 in (1, 2) else "PROGRAMADO"
            urgencia = "SI" if ingreso_tipo == "URGENCIA" else "NO"
            urg_tipo = "NO_REGISTRADO"
            if ingreso_tipo == "URGENCIA":
                urg_tipo = RND.choice(main.URGENCIA_TIPO_OPTIONS) if hasattr(main, "URGENCIA_TIPO_OPTIONS") else RND.choice(
                    ["URGENCIA QUIRURGICA", "COMPLEMENTACION DIAGNOSTICA", "NO REALIZO TRAMITE ADMINISTRATIVO CORRESPONDIENTE"]
                )

            hosp = main.HospitalizacionDB(
                consulta_id=c.id,
                fecha_ingreso=ingreso,
                fecha_egreso=egreso,
                motivo="INGRESO DEMO",
                servicio="UROLOGIA",
                cama=f"{13 + (i % 18)}",
                nss=c.nss,
                agregado_medico=c.agregado_medico,
                nombre_completo=_safe_upper(c.nombre),
                edad=c.edad,
                sexo=_safe_upper(c.sexo),
                diagnostico=_safe_upper(c.diagnostico_principal),
                hgz_envio=RND.choice(["HGZ 27", "HGZ 24", "HGR 1", "HGZ 76"]),
                estatus_detalle=RND.choice(["ESTABLE", "DELICADO", "GRAVE"]),
                dias_hospitalizacion=max((today - ingreso).days, 0),
                dias_postquirurgicos=max((today - ingreso).days - 1, 0),
                incapacidad="SI" if i % 6 == 0 else "NO",
                incapacidad_emitida="NO",
                programado="SI" if ingreso_tipo == "PROGRAMADO" else "NO",
                medico_programado=f"DR. DEMO {1 + (i % 6)}",
                turno_programado=RND.choice(["MATUTINO", "VESPERTINO", "NOCTURNO"]),
                urgencia=urgencia,
                urgencia_tipo=urg_tipo,
                ingreso_tipo=ingreso_tipo,
                estado_clinico=RND.choice(["ESTABLE", "DELICADO", "GRAVE"]),
                uci="SI" if i % 10 == 0 else "NO",
                observaciones="REGISTRO DEMO",
                estatus="EGRESADO" if egreso else "ACTIVO",
            )
            db.add(hosp)

            lab_day = ingreso
            for _ in range(2):
                values = {
                    "CREATININA": round(RND.uniform(0.7, 2.4), 2),
                    "HEMOGLOBINA": round(RND.uniform(7.2, 14.8), 1),
                    "LEUCOCITOS": int(RND.uniform(4500, 17500)),
                    "PLAQUETAS": int(RND.uniform(95, 380)),
                    "SODIO": round(RND.uniform(128, 149), 1),
                    "POTASIO": round(RND.uniform(3.0, 6.1), 1),
                }
                for test_name, value in values.items():
                    db.add(
                        main.LabDB(
                            consulta_id=c.id,
                            patient_id=str(c.id),
                            timestamp=datetime.combine(lab_day, datetime.min.time()),
                            test_code=test_name[:6],
                            test_name=test_name,
                            value=str(value),
                            unit="",
                            source="DEMO",
                        )
                    )
                lab_day = min(lab_day + timedelta(days=1), today)

        db.commit()

        # 3) Programadas (12) + postqx parcial.
        for i, c in enumerate(created_consultas[:12]):
            proc = RND.choice(main.QUIROFANO_PROCEDIMIENTOS)
            diag = _safe_upper(c.diagnostico_principal)
            qx_date = today - timedelta(days=RND.randint(0, 10))
            cirugia = main.QuirofanoDB(
                consulta_id=c.id,
                fecha_programada=qx_date,
                procedimiento=proc,
                cirujano=RND.choice(["DR. ALFA", "DR. BRAVO", "DRA. CHARLIE", "DR. DELTA"]),
                anestesiologo=RND.choice(["DRA. A1", "DR. A2", "DRA. A3"]),
                quirofano=RND.choice(["QX-1", "QX-2", "QX-3"]),
                estatus="PROGRAMADA",
                notas=f"HALLAZGOS PREOP DEMO {i+1}",
            )
            db.add(cirugia)
            db.flush()

            main.sync_quirofano_to_surgical_db(
                c,
                cirugia,
                extra_fields={
                    "nss": c.nss,
                    "agregado_medico": c.agregado_medico,
                    "paciente_nombre": _safe_upper(c.nombre),
                    "edad": c.edad,
                    "edad_grupo": main.classify_age_group(c.edad),
                    "sexo": _safe_upper(c.sexo),
                    "grupo_sexo": _safe_upper(c.sexo),
                    "patologia": diag,
                    "diagnostico_principal": diag,
                    "grupo_patologia": main.classify_pathology_group(diag),
                    "procedimiento": proc,
                    "procedimiento_programado": proc,
                    "grupo_procedimiento": main.classify_procedure_group(proc, "", ""),
                    "hgz": RND.choice(["HGZ 27", "HGZ 24", "HGR 1"]),
                    "estatus": "PROGRAMADA",
                    "modulo_origen": "quirofano",
                },
            )
            created_programadas += 1

        db.commit()

        prog_rows = (
            sdb.query(main.SurgicalProgramacionDB)
            .filter(main.SurgicalProgramacionDB.nss.in_([c.nss for c in created_consultas[:12]]))
            .order_by(main.SurgicalProgramacionDB.id.asc())
            .all()
        )
        for idx, row in enumerate(prog_rows):
            if idx % 4 == 0:
                continue
            fecha_real = (row.fecha_programada or today) + timedelta(days=RND.randint(0, 2))
            if fecha_real > today:
                fecha_real = today
            sangrado = round(RND.uniform(30, 550), 1)
            hallazgo = RND.choice(
                [
                    "SIN INCIDENTES TRANSOPERATORIOS",
                    "ADHERENCIAS + SANGRADO VENOSO CONTROLADO",
                    "LITO IMPACTADO CON EDEMA LOCAL",
                    "TUMOR LOCALMENTE AVANZADO CON RODETE VESICAL",
                ]
            )
            cir = RND.choice(["DR. ALFA", "DR. BRAVO", "DRA. CHARLIE", "DR. DELTA"])
            row.estatus = "REALIZADA"
            row.fecha_realizacion = fecha_real
            row.fecha_postquirurgica = fecha_real
            row.cirujano = cir
            row.sangrado_ml = sangrado
            row.diagnostico_postop = row.patologia
            row.procedimiento_realizado = row.procedimiento_programado
            row.complicaciones_postquirurgicas = hallazgo
            row.nota_postquirurgica = f"Hallazgos quirúrgicos: {hallazgo}. Evolución inmediata estable."

            sdb.add(
                main.SurgicalPostquirurgicaDB(
                    surgical_programacion_id=row.id,
                    quirofano_id=row.quirofano_id,
                    consulta_id=row.consulta_id,
                    fecha_realizacion=fecha_real,
                    cirujano=cir,
                    sangrado_ml=sangrado,
                    diagnostico_postop=row.diagnostico_postop,
                    procedimiento_realizado=row.procedimiento_realizado,
                    complicaciones=hallazgo,
                    nota_postquirurgica=f"Hallazgos quirúrgicos: {hallazgo}.",
                )
            )
            created_postqx += 1

        # 4) Urgencias (8) + postqx parcial.
        for i, c in enumerate(created_consultas[12:], start=1):
            diag = _safe_upper(c.diagnostico_principal)
            proc = RND.choice(main.QUIROFANO_PROCEDIMIENTOS)
            fecha_urg = today - timedelta(days=RND.randint(0, 8))
            grupo_pat = main.classify_pathology_group(diag)
            grupo_proc = main.classify_procedure_group(proc, "", "")
            urg = main.SurgicalUrgenciaProgramacionDB(
                consulta_id=c.id,
                nss=c.nss,
                agregado_medico=c.agregado_medico,
                paciente_nombre=_safe_upper(c.nombre),
                edad=c.edad,
                edad_grupo=main.classify_age_group(c.edad),
                sexo=_safe_upper(c.sexo),
                grupo_sexo=_safe_upper(c.sexo),
                patologia=diag,
                patologia_cie10=main.get_cie10_from_patologia(diag),
                grupo_patologia=grupo_pat,
                procedimiento_programado=proc,
                grupo_procedimiento=grupo_proc,
                insumos_solicitados=RND.choice(["EQUIPO DE CIRUGIA ABIERTA", "ENDOURO (INTERMED)"]),
                requiere_intermed="SI" if i % 2 == 0 else "NO",
                hgz=RND.choice(["HGZ 27", "HGZ 24", "HGR 1"]),
                estatus="PROGRAMADA",
                fecha_urgencia=fecha_urg,
                modulo_origen="QUIROFANO_URGENCIA",
            )
            sdb.add(urg)
            sdb.flush()

            mirrored = main.SurgicalProgramacionDB(
                quirofano_id=-(100000 + int(urg.id)),
                consulta_id=c.id,
                curp=c.curp,
                nss=c.nss,
                agregado_medico=c.agregado_medico,
                paciente_nombre=_safe_upper(c.nombre),
                edad=c.edad,
                edad_grupo=main.classify_age_group(c.edad),
                sexo=_safe_upper(c.sexo),
                grupo_sexo=_safe_upper(c.sexo),
                diagnostico_principal=diag,
                patologia=diag,
                grupo_patologia=grupo_pat,
                procedimiento=proc,
                procedimiento_programado=proc,
                grupo_procedimiento=grupo_proc,
                insumos_solicitados=urg.insumos_solicitados,
                requiere_intermed=urg.requiere_intermed,
                hgz=urg.hgz,
                fecha_programada=fecha_urg,
                estatus="PROGRAMADA",
                protocolo_completo="SI",
                pendiente_programar="NO",
                modulo_origen="QUIROFANO_URGENCIA",
                urgencia_programacion_id=urg.id,
            )
            sdb.add(mirrored)
            sdb.flush()
            urg.surgical_programacion_id = mirrored.id
            created_urgencias += 1

            if i % 3 != 0:
                fecha_real = fecha_urg + timedelta(days=RND.randint(0, 1))
                if fecha_real > today:
                    fecha_real = today
                sangrado = round(RND.uniform(40, 620), 1)
                hallazgo = RND.choice(
                    [
                        "URGENCIA QUIRURGICA RESUELTA SIN EVENTOS",
                        "SEPSIS URINARIA CONTROLADA POST DRENAJE",
                        "OBSTRUCCION URETERAL CON LITOTRICIA EXITOSA",
                    ]
                )
                cir = RND.choice(["DR. BRAVO", "DRA. CHARLIE", "DR. DELTA"])
                mirrored.estatus = "REALIZADA"
                mirrored.fecha_realizacion = fecha_real
                mirrored.fecha_postquirurgica = fecha_real
                mirrored.cirujano = cir
                mirrored.sangrado_ml = sangrado
                mirrored.diagnostico_postop = diag
                mirrored.procedimiento_realizado = proc
                mirrored.complicaciones_postquirurgicas = hallazgo
                mirrored.nota_postquirurgica = f"Hallazgos quirúrgicos: {hallazgo}."

                urg.estatus = "REALIZADA"
                urg.fecha_realizacion = fecha_real
                urg.cirujano = cir
                urg.sangrado_ml = sangrado
                urg.diagnostico_postop = diag
                urg.procedimiento_realizado = proc
                urg.complicaciones_postquirurgicas = hallazgo
                urg.nota_postquirurgica = f"Hallazgos quirúrgicos: {hallazgo}."

                sdb.add(
                    main.SurgicalPostquirurgicaDB(
                        surgical_programacion_id=mirrored.id,
                        quirofano_id=mirrored.quirofano_id,
                        consulta_id=mirrored.consulta_id,
                        fecha_realizacion=fecha_real,
                        cirujano=cir,
                        sangrado_ml=sangrado,
                        diagnostico_postop=diag,
                        procedimiento_realizado=proc,
                        complicaciones=hallazgo,
                        nota_postquirurgica=f"Hallazgos quirúrgicos: {hallazgo}.",
                    )
                )
                created_postqx += 1

        sdb.commit()

        demo_nss = [c.nss for c in created_consultas]
        return {
            "ok": True,
            "seed": seed,
            "created_consultas": len(created_consultas),
            "created_programadas": created_programadas,
            "created_urgencias": created_urgencias,
            "created_postqx": created_postqx,
            "sample_nss": demo_nss[:5],
        }
    except Exception as exc:
        db.rollback()
        sdb.rollback()
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()
        sdb.close()


def main_cli() -> None:
    parser = argparse.ArgumentParser(description="Carga demo de 20 pacientes para validar flujo RNP.")
    parser.add_argument("--count", type=int, default=20, help="Cantidad de pacientes demo a crear")
    parser.add_argument("--seed", type=int, default=None, help="Semilla aleatoria fija")
    args = parser.parse_args()
    result = seed_demo_patients(count=max(1, int(args.count)), seed=args.seed)
    if not result.get("ok"):
        print("ERROR:", result.get("error"))
        raise SystemExit(1)
    print("DEMO_SEED_OK", result)


if __name__ == "__main__":
    main_cli()

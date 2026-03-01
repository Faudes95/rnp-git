# -*- coding: utf-8 -*-
"""Plantillas inline legadas extraidas de main.py para reducir deuda tecnica.

No cambia rutas ni comportamiento; solo desacopla strings largos del modulo principal.
"""

MENU_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Registro Nacional de Pacientes - Urología CMNR</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        :root {
            --imss-verde-1: #0f3d37;
            --imss-verde-2: #0b2f2a;
            --imss-verde-3: #145045;
            --imss-dorado: #b28a47;
            --imss-claro: #f3f1ec;
            --imss-texto: #102f2f;
            --imss-gris: #49545a;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Montserrat', sans-serif;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            position: relative;
            overflow-x: hidden;
            background-image:
                linear-gradient(rgba(8, 30, 31, 0.28), rgba(8, 30, 31, 0.42)),
                url('{{ hospital_bg_url }}');
            background-size: cover;
            background-position: center 78%;
            background-attachment: fixed;
        }
        body::before {
            content: "";
            position: fixed;
            inset: 162px 0 52px 0;
            background:
                linear-gradient(130deg, rgba(255,255,255,0.08) 12%, transparent 12%, transparent 34%, rgba(255,255,255,0.06) 34%, rgba(255,255,255,0.06) 52%, transparent 52%),
                linear-gradient(160deg, transparent 0%, transparent 64%, rgba(0,0,0,0.18) 64%, rgba(0,0,0,0.18) 100%);
            pointer-events: none;
            z-index: 0;
        }
        .page-shell {
            flex: 1;
            display: flex;
            flex-direction: column;
            position: relative;
            z-index: 1;
        }
        .header-bar {
            background-color: rgba(255, 255, 255, 0.97);
            border-bottom: 4px solid var(--imss-dorado);
            padding: 10px 30px;
            display: grid;
            grid-template-columns: 320px minmax(0, 1fr) 320px;
            align-items: center;
            gap: 10px;
            min-height: 154px;
            box-shadow: 0 6px 20px rgba(0,0,0,0.22);
            z-index: 10;
            position: relative;
        }
        .header-bar::before {
            content: "";
            position: absolute;
            inset: 0;
            background-color: #ffffff;
            background-image: url('{{ imss_pattern_url }}'), url('{{ imss_logo_url }}');
            background-repeat: repeat-x, no-repeat;
            background-size: 84px auto, auto 280%;
            background-position: left center, left -210px center;
            opacity: 0.42;
            filter: grayscale(1) brightness(2.35) contrast(0.58);
            pointer-events: none;
        }
        .header-bar::after {
            content: "";
            position: absolute;
            inset: 0;
            background: rgba(255, 255, 255, 0.24);
            pointer-events: none;
        }
        .logo-block {
            display: flex;
            align-items: center;
            justify-content: center;
            flex: 0 0 auto;
            position: relative;
            z-index: 1;
        }
        .logo-block-imss {
            width: auto;
            min-height: 0;
            justify-content: flex-start;
            justify-self: start;
            padding: 0;
            border: 0;
            background: transparent;
            box-shadow: none;
        }
        .logo-block-uro {
            width: auto;
            justify-content: flex-end;
            justify-self: end;
        }
        .logo-imss {
            max-height: 176px;
            max-width: 292px;
            width: auto;
            object-fit: contain;
            filter: drop-shadow(0 2px 6px rgba(0,0,0,0.25));
        }
        .logo-urologia {
            max-height: 104px;
            max-width: 104px;
            width: auto;
            object-fit: contain;
            display: block;
            border-radius: 50%;
            border: 1.5px solid rgba(178,138,71,0.9);
            background: rgba(255,255,255,0.88);
            padding: 2px;
            filter: drop-shadow(0 2px 7px rgba(0,0,0,0.22));
        }
        .hospital-title {
            flex: 1 1 auto;
            min-width: 0;
            width: 100%;
            color: var(--imss-texto);
            text-align: center;
            text-transform: uppercase;
            display: flex;
            flex-direction: column;
            justify-content: center;
            line-height: 1.12;
            position: relative;
            z-index: 1;
            justify-self: center;
        }
        .hospital-title h1 {
            margin: 0;
            font-size: 33px;
            font-weight: 800;
            letter-spacing: 1px;
            line-height: 1.1;
        }
        .hospital-title h2 {
            margin: 9px 0 0 0;
            font-size: 22px;
            font-weight: 700;
            color: var(--imss-dorado);
            letter-spacing: 1.8px;
        }
        .main-content {
            flex: 1;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 24px 18px;
            position: relative;
            z-index: 5;
        }
        .card-menu {
            background: #ffffff;
            padding: 22px 22px 24px;
            border-radius: 22px;
            box-shadow: 0 28px 48px rgba(0,0,0,0.34);
            text-align: center;
            max-width: 1000px;
            width: 100%;
            border: 4px solid var(--imss-dorado);
            position: relative;
            overflow: hidden;
        }
        .card-menu::before {
            content: "";
            position: absolute;
            inset: 0;
            background-image: url('{{ imss_logo_url }}');
            background-repeat: repeat;
            background-size: 120px auto;
            background-position: center center;
            opacity: 0.05;
            filter: grayscale(1) brightness(2) contrast(0.7);
            pointer-events: none;
        }
        .card-menu h1 {
            color: var(--imss-texto);
            font-size: 34px;
            margin-bottom: 8px;
            text-transform: uppercase;
            font-weight: 800;
            letter-spacing: 1px;
            position: relative;
            z-index: 1;
        }
        .card-menu h2.subtitle {
            color: var(--imss-dorado);
            font-size: 20px;
            margin-top: 0;
            margin-bottom: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1.4px;
            position: relative;
            z-index: 1;
        }
        .grid-buttons {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 14px;
            border-top: none;
            padding-top: 8px;
            position: relative;
            z-index: 1;
        }
        .btn-menu {
            background: #f7f5f0;
            border: 2px solid var(--imss-dorado);
            border-radius: 14px;
            padding: 14px 10px 12px;
            text-decoration: none;
            color: var(--imss-texto);
            transition: transform 0.22s ease, box-shadow 0.22s ease, border-color 0.22s ease;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 9px;
            min-height: 142px;
            box-shadow: 0 5px 12px rgba(0,0,0,0.08);
        }
        .btn-menu:hover {
            transform: translateY(-4px);
            box-shadow: 0 14px 22px rgba(16,47,47,0.2);
            border-color: var(--imss-dorado);
        }
        .icon-badge {
            width: 74px;
            height: 74px;
            border-radius: 50%;
            border: 2px solid var(--imss-dorado);
            background: radial-gradient(circle at 30% 25%, #196157, var(--imss-verde-1) 62%);
            display: inline-flex;
            align-items: center;
            justify-content: center;
            box-shadow: inset 0 0 0 1px rgba(255,255,255,0.16);
        }
        .icon-svg {
            width: 36px;
            height: 36px;
            stroke: #ffffff;
            fill: none;
            stroke-width: 2.4;
            stroke-linecap: round;
            stroke-linejoin: round;
        }
        .icon-svg .accent { fill: #f24839; stroke: #f24839; }
        .btn-text {
            font-weight: 700;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            line-height: 1.05;
        }
        .btn-reporte { grid-column: 3; }

        .global-footer {
            width: 100%;
            text-align: center;
            background: #ffffff;
            color: var(--imss-verde-1);
            border-top: 2px solid var(--imss-dorado);
            padding: 14px 10px;
            font-size: 14px;
            font-weight: 700;
            letter-spacing: 0.7px;
            text-transform: uppercase;
            position: relative;
            z-index: 2;
            overflow: hidden;
        }
        .global-footer::before {
            content: "";
            position: absolute;
            inset: 0;
            background-color: #ffffff;
            background-image: url('{{ imss_pattern_url }}');
            background-repeat: repeat-x;
            background-size: 88px auto;
            background-position: left center;
            opacity: 0.34;
            filter: grayscale(1) brightness(2.35) contrast(0.58);
            pointer-events: none;
        }
        .global-footer .footer-text {
            position: relative;
            z-index: 1;
        }
        @media (max-width: 1024px) {
            .header-bar { padding: 10px 14px; min-height: 128px; grid-template-columns: 250px 1fr 250px; }
            .logo-block-imss { width: auto; }
            .logo-block-uro { width: auto; }
            .logo-imss { max-height: 128px; max-width: 224px; }
            .logo-urologia { max-height: 102px; max-width: 102px; }
            .hospital-title h1 { font-size: 26px; }
            .hospital-title h2 { font-size: 18px; }
            .card-menu { max-width: 900px; }
            .card-menu h1 { font-size: 30px; }
            .card-menu h2.subtitle { font-size: 18px; }
            .btn-text { font-size: 16px; }
            .grid-buttons { grid-template-columns: repeat(2, 1fr); }
            .btn-reporte { grid-column: 1 / -1; }
            .global-footer { font-size: 13px; }
        }
        @media (max-width: 760px) {
            body::before { inset: 210px 0 48px 0; }
            .header-bar {
                display: flex;
                flex-wrap: wrap;
                justify-content: center;
                gap: 12px;
                padding-bottom: 12px;
            }
            .logo-block-imss {
                width: 100%;
                max-width: 220px;
                order: 2;
                justify-content: center;
            }
            .logo-block-uro { order: 3; width: 100%; }
            .hospital-title {
                order: 1;
                flex: 1 1 100%;
            }
            .hospital-title h1 {
                font-size: 19px;
                letter-spacing: 0.4px;
            }
            .hospital-title h2 {
                font-size: 16px;
                letter-spacing: 0.8px;
            }
            .card-menu {
                padding: 16px 12px 18px;
                border-radius: 16px;
            }
            .card-menu h1 { font-size: 24px; }
            .card-menu h2.subtitle {
                margin-bottom: 14px;
                font-size: 14px;
            }
            .grid-buttons { grid-template-columns: 1fr; gap: 14px; }
            .btn-menu { min-height: 122px; }
            .btn-text { font-size: 15px; }
            .icon-badge { width: 62px; height: 62px; }
            .icon-svg { width: 33px; height: 33px; }
            .global-footer { font-size: 12px; }
        }
    </style>
</head>
<body>
    <div class="page-shell">
        <div class="header-bar">
            <div class="logo-block logo-block-imss">
                <img src="{{ imss_logo_url }}" class="logo-imss" alt="Logo IMSS">
            </div>
            <div class="hospital-title">
                <h1>HOSPITAL DE ESPECIALIDADES DR. ANTONIO LA FRAGA MOURET</h1>
                <h2>CENTRO MEDICO NACIONAL LA RAZA</h2>
            </div>
            <div class="logo-block logo-block-uro">
                <img src="{{ urologia_logo_url }}" class="logo-urologia" alt="Logo Urología">
            </div>
        </div>
        <div class="main-content">
            <div class="card-menu">
                <h1>REGISTRO NACIONAL DE PACIENTES</h1>
                <h2 class="subtitle">(SERVICIO DE UROLOGÍA)</h2>
                <div class="grid-buttons">
                    <a href="/consulta_externa" class="btn-menu">
                        <span class="icon-badge">
                            <svg class="icon-svg" viewBox="0 0 64 64" aria-hidden="true">
                                <path d="M19 12v11a9 9 0 0 0 18 0V12"></path>
                                <path d="M28 32v8a9 9 0 0 0 18 0"></path>
                                <circle cx="46" cy="40" r="6"></circle>
                                <path d="M23 18h-4M33 18h-4"></path>
                            </svg>
                        </span>
                        <span class="btn-text">CONSULTA EXTERNA</span>
                    </a>
                    <a href="/hospitalizacion" class="btn-menu">
                        <span class="icon-badge">
                            <svg class="icon-svg" viewBox="0 0 64 64" aria-hidden="true">
                                <path d="M10 36h34a4 4 0 0 1 4 4v4H10z"></path>
                                <path d="M10 44v7M48 44v7"></path>
                                <path d="M14 36V25h10a6 6 0 0 1 6 6v5"></path>
                                <rect class="accent" x="43" y="11" width="11" height="11" rx="1"></rect>
                                <path d="M48.5 13.5v6M45.5 16.5h6"></path>
                            </svg>
                        </span>
                        <span class="btn-text">HOSPITALIZACIÓN</span>
                    </a>
                    <a href="/quirofano" class="btn-menu">
                        <span class="icon-badge">
                            <svg class="icon-svg" viewBox="0 0 64 64" aria-hidden="true">
                                <path d="M14 46l19-19"></path>
                                <path d="M30 30l7 7"></path>
                                <path d="M37 37l13-13a5 5 0 0 0 0-7l-1-1"></path>
                                <path d="M14 46h8l17-17-8-8-17 17z"></path>
                            </svg>
                        </span>
                        <span class="btn-text">QUIRÓFANO</span>
                    </a>
                    <a href="/expediente" class="btn-menu">
                        <span class="icon-badge">
                            <svg class="icon-svg" viewBox="0 0 64 64" aria-hidden="true">
                                <path d="M9 24h18l5 5h23v22H9z"></path>
                                <path d="M9 24v-8h18l5 5h23v8"></path>
                            </svg>
                        </span>
                        <span class="btn-text">EXPEDIENTE CLÍNICO ÚNICO</span>
                    </a>
                    <a href="/busqueda" class="btn-menu">
                        <span class="icon-badge">
                            <svg class="icon-svg" viewBox="0 0 64 64" aria-hidden="true">
                                <circle cx="27" cy="27" r="12"></circle>
                                <path d="M36 36l14 14"></path>
                            </svg>
                        </span>
                        <span class="btn-text">BÚSQUEDA</span>
                    </a>
                    <a href="/reporte" class="btn-menu btn-reporte">
                        <span class="icon-badge">
                            <svg class="icon-svg" viewBox="0 0 64 64" aria-hidden="true">
                                <path d="M12 50h40"></path>
                                <rect x="17" y="31" width="7" height="19"></rect>
                                <rect x="29" y="24" width="7" height="26"></rect>
                                <rect x="41" y="18" width="7" height="32"></rect>
                            </svg>
                        </span>
                        <span class="btn-text">REPORTE ESTADISTICO</span>
                    </a>
                </div>
            </div>
        </div>
    </div>
    <div class="global-footer"><span class="footer-text">2026 PROGRAMA PILOTO (TODOS LOS DERECHOS RESERVADOS)</span></div>
</body>
</html>
"""

# =============================================================================
# PLANTILLA DEL FORMULARIO COMPLETO (EXTRAÍDA DIRECTAMENTE DEL CÓDIGO MAESTRO)
# =============================================================================
CONSULTA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Consulta Externa - Urología</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        :root { --imss-verde: #13322B; --imss-dorado: #B38E5D; --fondo-gris: #f4f6f9; }
        body { font-family: 'Montserrat', sans-serif; background: var(--fondo-gris); margin: 0; padding: 20px; }
        .main-container { max-width: 1200px; margin: auto; background: white; border-top: 6px solid var(--imss-verde); border-radius: 8px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); overflow: hidden; }
        .header-form { background: white; padding: 30px 40px; border-bottom: 2px solid var(--imss-dorado); display: flex; justify-content: space-between; align-items: center; }
        .header-form h2 { margin: 0; color: var(--imss-verde); text-transform: uppercase; font-size: 22px; font-weight: 800; }
        .back-btn { text-decoration: none; color: var(--imss-verde); font-weight: 700; border: 2px solid var(--imss-verde); padding: 10px 20px; border-radius: 5px; transition: all 0.3s; }
        .back-btn:hover { background: var(--imss-verde); color: white; }
        form { padding: 40px; }
        fieldset { border: 1px solid #e0e0e0; padding: 25px; margin-bottom: 35px; border-radius: 6px; background-color: #fff; }
        legend { font-weight: 700; color: white; background: var(--imss-verde); padding: 8px 20px; border-radius: 20px; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; box-shadow: 0 2px 5px rgba(0,0,0,0.2); }
        .section-title-internal { color: var(--imss-verde); font-size: 15px; font-weight: 700; margin-top: 25px; margin-bottom: 15px; border-bottom: 1px solid #eee; padding-bottom: 5px; text-transform: uppercase;}
        .row { display: flex; gap: 20px; margin-bottom: 18px; flex-wrap: wrap; }
        .col-1 { flex: 1 1 100%; } .col-2 { flex: 1 1 45%; } .col-3 { flex: 1 1 30%; } .col-4 { flex: 1 1 22%; }
        label { display: block; font-size: 11px; font-weight: 700; color: #555; margin-bottom: 6px; text-transform: uppercase; }
        input, select, textarea { width: 100%; padding: 12px; border: 1px solid #ccc; border-radius: 4px; font-size: 13px; font-family: 'Montserrat', sans-serif; }
        input:focus, select:focus, textarea:focus { border-color: var(--imss-verde); outline: none; box-shadow: 0 0 0 2px rgba(19, 50, 43, 0.1); }
        .calculated-field { background-color: #e8f5e9; color: #2e7d32; font-weight: bold; border: 1px solid #c8e6c9; }
        .save-btn { background-color: var(--imss-verde); color: white; width: 100%; padding: 20px; font-size: 18px; font-weight: 800; border: none; border-radius: 6px; cursor: pointer; margin-top: 30px; transition: 0.3s; text-transform: uppercase; letter-spacing: 1px; }
        .save-btn:hover { background-color: #0e2621; box-shadow: 0 5px 15px rgba(0,0,0,0.3); }
        .dynamic-section { display: none; background: #fcfcfc; padding: 25px; border-radius: 8px; border: 1px solid #ddd; border-left: 5px solid var(--imss-dorado); margin-top: 20px; box-shadow: inset 0 0 10px rgba(0,0,0,0.02); }

        /* Toggle Switch Style */
        .toggle-container { display: flex; justify-content: center; margin-bottom: 30px; background: #e0f2f1; padding: 15px; border-radius: 10px; }
        .toggle-btn { padding: 10px 30px; border: 2px solid var(--imss-verde); cursor: pointer; font-weight: bold; color: var(--imss-verde); background: white; margin: 0 5px; border-radius: 5px; }
        .toggle-btn.active { background: var(--imss-verde); color: white; }

        #seccion_subsecuente { display: none; }
    </style>
</head>
<body>
    <div class="main-container">
        <div class="header-form">
            <h2>Consulta Externa</h2>
            <a href="/" class="back-btn">← MENÚ PRINCIPAL</a>
        </div>

        <div class="toggle-container">
            <div class="toggle-btn active" onclick="toggleConsultaTipo('primera')" id="btn-primera">CONSULTA PRIMERA VEZ</div>
            <div class="toggle-btn" onclick="toggleConsultaTipo('subsecuente')" id="btn-subsecuente">CONSULTA SUBSECUENTE</div>
        </div>

        <form action="/guardar_consulta_completa" method="post" enctype="multipart/form-data">
            <input type="hidden" name="csrf_token" value="{{ csrf_token }}">

            <div id="seccion_primera_vez">

                <fieldset>
                    <legend>1. Ficha de Identificación</legend>
                    <div class="row">
                        <div class="col-3"><label>CURP *</label><input type="text" name="curp" required style="text-transform: uppercase;"></div>
                        <div class="col-3"><label>NSS</label><input type="text" name="nss" maxlength="10" pattern="\\d{10}" inputmode="numeric" placeholder="10 dígitos"></div>
                        <div class="col-3"><label>Agregado Médico</label><input type="text" name="agregado_medico"></div>
                    </div>
                    <div class="row">
                        <div class="col-2"><label>Nombre Completo (Paterno Materno Nombres) *</label><input type="text" name="nombre" required style="text-transform: uppercase;" placeholder="APELLIDO PATERNO / MATERNO / NOMBRES"></div>
                        <div class="col-4"><label>Fecha Nac. (DD/MM/AAAA)</label><input type="date" name="fecha_nacimiento"></div>
                        <div class="col-4"><label>Edad</label><input type="number" name="edad"></div>
                    </div>
                    <div class="row">
                         <div class="col-4"><label>Sexo</label><select name="sexo"><option value="">Seleccionar...</option><option>Masculino</option><option>Femenino</option></select></div>
                        <div class="col-4"><label>Tipo Sangre</label>
                            <select name="tipo_sangre">
                                <option>Se desconoce</option>
                                <option>O+</option><option>O-</option><option>A+</option><option>A-</option><option>B+</option><option>B-</option><option>AB+</option><option>AB-</option>
                            </select>
                        </div>
                        <div class="col-2"><label>Ocupación Actual</label>
                            <select name="ocupacion">
                                <optgroup label="TRABAJADOR DE LA SALUD"><option>Médico</option><option>Enfermero(a)</option><option>Psicólogo</option><option>Nutriólogo</option><option>Odontólogo</option><option>Otro técnico salud</option></optgroup>
                                <optgroup label="PROFESIONISTAS"><option>Abogado</option><option>Contador</option><option>Arquitecto</option><option>Ingeniero</option><option>Músico</option></optgroup>
                                <optgroup label="EMPLEADO"><option>Empresa Privada (Especificar)</option><option>Empresa Pública (Especificar)</option></optgroup>
                                <optgroup label="COMERCIANTE"><option>Ambulante</option><option>Comercio Nacional</option><option>Comercio Internacional</option></optgroup>
                                <option>Desempleado</option>
                                <optgroup label="JUBILADO/PENSIONADO"><option>Pensionado</option><option>No pensionado</option></optgroup>
                            </select>
                        </div>
                    </div>
                    <div class="row">
                        <div class="col-4"><label>Empresa (Si aplica)</label><input type="text" name="nombre_empresa" placeholder="Nombre de la empresa"></div>
                        <div class="col-4"><label>Escolaridad</label><select name="escolaridad"><option>Primaria</option><option>Secundaria</option><option>Preparatoria/Bachillerato</option><option>Licenciatura</option><option>Maestría</option><option>Especialidad</option><option>Doctorado</option><option>Sin escolaridad</option></select></div>
                    </div>

                    <div class="section-title-internal">Dirección y Contacto</div>
                    <div class="row">
                        <div class="col-4"><label>CP (Zonas)</label><input type="text" name="cp"></div>
                        <div class="col-3"><label>Alcaldía/Municipio/Foráneo</label>
                            <select name="alcaldia" id="select_alcaldia" onchange="toggleForaneo()">
                                <optgroup label="CDMX"><option>Azcapotzalco</option><option>Gustavo A. Madero</option><option>Cuauhtémoc</option><option>Miguel Hidalgo</option><option>Venustiano Carranza</option><option>Iztacalco</option><option>Benito Juárez</option><option>Coyoacán</option><option>Iztapalapa</option><option>Tlalpan</option><option>Magdalena Contreras</option><option>Cuajimalpa</option><option>Álvaro Obregón</option><option>Xochimilco</option><option>Milpa Alta</option><option>Tláhuac</option></optgroup>
                                <optgroup label="Edomex"><option>Ecatepec</option><option>Tlalnepantla</option><option>Naucalpan</option><option>Nezahualcóyotl</option><option>Chimalhuacán</option><option>Toluca</option><option>Metepec</option></optgroup>
                                <option value="foraneo">FORÁNEO</option>
                            </select>
                        </div>
                         <div class="col-2"><label>Colonia</label><input type="text" name="colonia"></div>
                    </div>
                    <div class="row" id="div_foraneo" style="display:none; background:#e0f7fa; padding:10px;">
                        <div class="col-1"><label>Especifique Estado/Municipio (Foráneo)</label><input type="text" name="estado_foraneo"></div>
                    </div>
                     <div class="row">
                        <div class="col-3"><label>Calle</label><input type="text" name="calle"></div>
                        <div class="col-4"><label>No. Ext</label><input type="text" name="no_ext"></div>
                        <div class="col-4"><label>No. Int</label><input type="text" name="no_int"></div>
                        <div class="col-3"><label>Teléfono</label><input type="tel" name="telefono"></div>
                        <div class="col-3"><label>Email</label><input type="email" name="email"></div>
                    </div>
                </fieldset>

                <fieldset>
                    <legend>2. Somatometría y Signos Vitales</legend>
                    <div class="row">
                        <div class="col-4"><label>Peso (kg)</label><input type="number" step="0.1" id="peso" name="peso" oninput="calcularIMC()"></div>
                        <div class="col-4"><label>Talla (cm)</label><input type="number" id="talla" name="talla" oninput="calcularIMC()"></div>
                        <div class="col-4"><label>IMC (Auto)</label><input type="text" id="imc" name="imc" readonly class="calculated-field"></div>
                        <div class="col-4"><label>T/A</label><input type="text" name="ta"></div>
                        <div class="col-4"><label>Frec. Cardíaca</label><input type="number" name="fc"></div>
                        <div class="col-4"><label>Temp.</label><input type="number" step="0.1" name="temp"></div>
                    </div>
                </fieldset>

                <fieldset>
                    <legend>3. Antecedentes Heredofamiliares</legend>
                    <div class="row">
                        <div class="col-1">
                            <label>Seleccione SI/NO</label>
                            <select id="ahf_select" name="ahf_status" onchange="toggleAHF()"><option value="no">Negados</option><option value="si">SI</option></select>
                        </div>
                    </div>
                    <div id="ahf_detalles" style="display:none; background:#f0f4c3; padding:15px; border-radius:5px;">
                        <div class="row">
                            <div class="col-2"><label>Línea</label><select name="ahf_linea"><option>Materna</option><option>Paterna</option><option>Ambas</option></select></div>
                            <div class="col-2"><label>Padecimiento</label><input type="text" name="ahf_padecimiento" placeholder="Escribir manual"></div>
                            <div class="col-2"><label>Estatus</label><select name="ahf_estatus"><option>Finado por el padecimiento</option><option>Vive con el padecimiento</option></select></div>
                        </div>
                    </div>
                </fieldset>

                <fieldset>
                    <legend>4. Personales Patológicos</legend>
                    <div class="section-title-internal">Patologías Crónicas <span style="cursor:pointer; color:blue; font-size:18px;" onclick="alert('Función para agregar más filas de patología activada')">[+] AGREGAR PATOLOGÍA</span></div>
                    <div class="row">
                        <div class="col-3"><label>Patología Crónica</label><input type="text" name="app_patologia"></div>
                        <div class="col-4"><label>Tiempo Evolución</label><input type="text" name="app_evolucion"></div>
                        <div class="col-3"><label>Tratamiento Actual</label><input type="text" name="app_tratamiento"></div>
                    </div>
                    <div class="row">
                        <div class="col-3"><label>Complicaciones</label>
                            <select id="app_complicaciones" name="app_complicaciones" onchange="toggleDisplay('app_complicaciones', 'div_complicaciones')"><option value="no">NO</option><option value="si">SI</option></select>
                        </div>
                        <div class="col-3" id="div_complicaciones" style="display:none;"><label>Describa Complicación</label><input type="text" name="app_desc_complicacion"></div>
                        <div class="col-3"><label>Lugar Seguimiento</label><input type="text" name="app_seguimiento"></div>
                        <div class="col-3"><label>Última Consulta</label><input type="date" name="app_ultima_consulta"></div>
                    </div>

                    <div class="section-title-internal">Hospitalizaciones Previas (Vinculación Mod 3)</div>
                    <div class="row">
                        <div class="col-4"><label>¿Hospitalizaciones?</label><select id="hosp_previas" name="hosp_previas" onchange="toggleDisplay('hosp_previas', 'div_hosp_detalles')"><option value="no">NO</option><option value="si">SI</option></select></div>
                    </div>
                    <div id="div_hosp_detalles" style="display:none; background:#e1bee7; padding:10px; border-radius:5px;">
                        <div class="row">
                            <div class="col-3"><label>Motivo</label><input type="text" name="hosp_motivo"></div>
                            <div class="col-4"><label>Días Estancia</label><input type="number" name="hosp_dias"></div>
                            <div class="col-4"><label>Ingreso a UCI</label><select name="hosp_uci"><option>NO</option><option>SI</option></select></div>
                            <div class="col-4"><label>Días UCI</label><input type="number" name="hosp_dias_uci"></div>
                        </div>
                    </div>

                    <div class="section-title-internal">Toxicomanías</div>
                    <div class="row" style="background: #fffbe6; padding: 15px; border-radius: 5px; border: 1px solid #ffe58f;">
                        <div class="col-4"><label>Tabaquismo</label><select id="tabaquismo_status" name="tabaquismo_status" onchange="toggleDisplay('tabaquismo_status', 'div_tabaco')"><option value="negativo">Negativo</option><option value="positivo">Positivo</option></select></div>
                        <div id="div_tabaco" style="display:none; width:100%;">
                            <div class="row">
                                <div class="col-4"><label>Cigarros/día</label><input type="number" id="cigarros_dia" name="cigarros_dia" oninput="calcularIT()"></div>
                                <div class="col-4"><label>Años fumando</label><input type="number" id="anios_fumando" name="anios_fumando" oninput="calcularIT()"></div>
                                <div class="col-4"><label>Índice Tabáquico</label><input type="text" id="indice_tabaquico" name="indice_tabaquico" readonly class="calculated-field"></div>
                            </div>
                        </div>
                    </div>
                    <div class="row" style="margin-top:10px;">
                        <div class="col-3"><label>Etilismo (Tiempo/Frec/Cant)</label><input type="text" name="alcoholismo" placeholder="Ej. 10 años, semanal, 3 copas"></div>
                        <div class="col-3"><label>Otras Drogas</label>
                            <select name="otras_drogas">
                                <option>Negadas</option><option>Marihuana</option><option>Cocaína</option><option>Metanfetaminas</option><option>Cristal</option><option>Opioides</option><option>Alucinógenos</option><option>Otras (Especificar)</option>
                            </select>
                        </div>
                        <div class="col-3"><label>Especifique Droga</label><input type="text" name="droga_manual"></div>
                    </div>

                    <div class="section-title-internal">Alergias y Transfusiones <span style="cursor:pointer; color:blue;" onclick="alert('Función agregar alergia +')">[+]</span></div>
                    <div class="row">
                        <div class="col-3"><label>Alérgeno</label><input type="text" name="alergeno"></div>
                        <div class="col-3"><label>Reacción</label><input type="text" name="alergia_reaccion"></div>
                        <div class="col-3"><label>Fecha Exposición</label><input type="date" name="alergia_fecha"></div>
                    </div>
                    <div class="row">
                        <div class="col-4"><label>Transfusiones</label><select id="transfusiones_status" name="transfusiones_status" onchange="toggleDisplay('transfusiones_status', 'div_transfusiones')"><option value="no">NO</option><option value="si">SI</option></select></div>
                        <div id="div_transfusiones" style="display:none; width:100%;">
                            <div class="row">
                                <div class="col-3"><label>Fecha Última</label><input type="date" name="trans_fecha"></div>
                                <div class="col-3"><label>Reacciones</label><input type="text" name="trans_reacciones"></div>
                            </div>
                        </div>
                    </div>
                </fieldset>

                <fieldset>
                    <legend>5. Antecedentes Quirúrgicos</legend>
                    <div class="row">
                        <div class="col-4"><label>Fecha Procedimiento</label><input type="date" name="aqx_fecha"></div>
                        <div class="col-2"><label>Procedimiento Realizado</label><input type="text" name="aqx_procedimiento"></div>
                        <div class="col-2"><label>Hallazgos</label><input type="text" name="aqx_hallazgos"></div>
                        <div class="col-2"><label>Médico</label><input type="text" name="aqx_medico"></div>
                    </div>
                     <div class="row">
                        <div class="col-4"><label>Complicaciones</label><select id="aqx_complicaciones_status" name="aqx_complicaciones_status" onchange="toggleDisplay('aqx_complicaciones_status', 'div_aqx_compli')"><option value="no">NO</option><option value="si">SI</option></select></div>
                        <div class="col-2" id="div_aqx_compli" style="display:none;"><label>Especifique Complicación</label><input type="text" name="aqx_desc_complicacion"></div>
                     </div>
                     <small style="color:var(--imss-dorado);">* Vinculado con Módulo de Quirófano (Avance 4)</small>
                </fieldset>

                <fieldset>
                    <legend>6. Padecimiento Actual y Exploración Física</legend>
                    <div class="row">
                        <div class="col-1"><label>Padecimiento Actual (PEEA)</label><textarea name="padecimiento_actual" rows="4"></textarea></div>
                    </div>
                    <div class="row">
                        <div class="col-1"><label>Exploración Física</label><textarea name="exploracion_fisica" rows="4" id="ef_textarea"></textarea></div>
                    </div>
                </fieldset>

                <fieldset style="border: 2px solid var(--imss-verde);">
                    <legend style="background: var(--imss-dorado); color: #13322B;">7. Diagnóstico Principal (CIE-11)</legend>
                    <div class="row" style="background: #e8f5e9; padding: 20px; border-radius: 8px;">
                        <div class="col-1">
                            <label style="font-size: 14px; color: var(--imss-verde);">SELECCIONE DIAGNÓSTICO PARA DESPLEGAR CAMPOS INTELIGENTES</label>
                            <select name="diagnostico_principal" id="diagnostico_principal" required style="font-size: 16px; padding: 15px; border: 2px solid var(--imss-verde);" onchange="mostrarFormularioDinamico()">
                                <option value="">-- SELECCIONAR --</option>
                                <optgroup label="ONCOLOGÍA UROLÓGICA">
                                    <option value="ca_rinon">CÁNCER DE RIÑÓN</option>
                                    <option value="ca_urotelial_alto">CÁNCER UROTELIAL TRACTO SUPERIOR</option>
                                    <option value="ca_vejiga">CÁNCER DE VEJIGA</option>
                                    <option value="ca_prostata">CÁNCER DE PRÓSTATA</option>
                                    <option value="ca_pene">CÁNCER DE PENE</option>
                                    <option value="ca_testiculo">CÁNCER DE TESTÍCULO</option>
                                    <option value="tumor_suprarrenal">TUMOR SUPRARRENAL</option>
                                    <option value="tumor_incierto_prostata">TUMOR COMPORTAMIENTO INCIERTO PRÓSTATA</option>
                                </optgroup>
                                <optgroup label="LITIASIS">
                                    <option value="litiasis_rinon">CÁLCULO DEL RIÑÓN</option>
                                    <option value="litiasis_ureter">CÁLCULO DEL URÉTER</option>
                                    <option value="litiasis_vejiga">CÁLCULO DE LA VEJIGA</option>
                                </optgroup>
                                <optgroup label="ANDROLOGÍA/UROGINE/TRANSPLANTE">
                                    <option value="priapismo">PRIAPISMO / DISFUNCIÓN ERÉCTIL</option>
                                    <option value="incontinencia">INCONTINENCIA URINARIA</option>
                                    <option value="fistula">FÍSTULA (V-V / U-V)</option>
                                    <option value="trasplante">TRASPLANTE RENAL (DONADOR VIVO)</option>
                                </optgroup>
                                <optgroup label="OTRAS">
                                    <option value="hpb">HIPERPLASIA PROSTÁTICA BENIGNA</option>
                                    <option value="infeccion">ABSCESO RENAL/PROSTÁTICO/PIELONEFRITIS</option>
                                </optgroup>
                            </select>
                        </div>
                    </div>

                    <div id="form_ca_rinon" class="dynamic-section">
                        <div class="section-title-internal">Protocolo Cáncer Renal</div>
                        <div class="row"><div class="col-4"><label>Tiempo Diagnóstico</label><input type="text" name="rinon_tiempo"></div></div>
                        <div class="row">
                            <div class="col-4"><label>TNM (Calcula Etapa)</label><input type="text" name="rinon_tnm"></div>
                            <div class="col-4"><label>Etapa Clínica</label><input type="text" name="rinon_etapa"></div>
                            <div class="col-4"><label>ECOG</label><input type="text" name="rinon_ecog"></div>
                            <div class="col-4"><label>Charlson (Auto APP)</label><input type="text" name="rinon_charlson"></div>
                        </div>
                        <div class="row">
                            <div class="col-3"><label>¿Nefrectomía Radical?</label><select name="rinon_nefrectomia"><option>NO</option><option>SI (Abierta)</option><option>SI (Laparoscópica)</option></select></div>
                            <div class="col-3"><label>RHP (Folio IMSS)</label><input type="text" name="rinon_rhp"></div>
                            <div class="col-3"><label>Tx Sistémico</label><input type="text" name="rinon_sistemico" placeholder="Fármaco, Dosis, Tiempo"></div>
                        </div>
                    </div>

                    <div id="form_ca_urotelial" class="dynamic-section">
                        <div class="section-title-internal">Protocolo UTUC</div>
                        <div class="row">
                            <div class="col-4"><label>Tiempo Dx</label><input type="text" name="utuc_tiempo"></div>
                            <div class="col-4"><label>TNM / Etapa</label><input type="text" name="utuc_tnm"></div>
                        </div>
                        <div class="row">
                            <div class="col-3"><label>Tx Quirúrgico</label><select name="utuc_tx_quirurgico"><option>NO</option><option>Abierto</option><option>Laparoscópico</option></select></div>
                            <div class="col-3"><label>RHP</label><input type="text" name="utuc_rhp"></div>
                            <div class="col-3"><label>Tx Sistémico/Reacciones</label><input type="text" name="utuc_sistemico"></div>
                        </div>
                    </div>

                    <div id="form_ca_vejiga" class="dynamic-section">
                        <div class="section-title-internal">Protocolo Cáncer Vejiga</div>
                        <div class="row">
                            <div class="col-4"><label>TNM (Calcula Etapa)</label><input type="text" name="vejiga_tnm"></div>
                            <div class="col-4"><label>ECOG</label><input type="text" name="vejiga_ecog"></div>
                        </div>
                        <div class="section-title-internal">Caracterización Hematuria</div>
                        <div class="row" style="background:#ffebee; padding:10px;">
                            <div class="col-3"><label>Tipo</label><select name="vejiga_hematuria_tipo"><option>Macro</option><option>Micro</option></select></div>
                            <div class="col-3"><label>Coágulos</label><select name="vejiga_coagulos_tipo" id="vejiga_coagulos_tipo" onchange="syncCoagulos()"><option>Formadora</option><option>No formadora</option></select></div>
                            <input type="hidden" name="vejiga_hematuria_coagulos" id="vejiga_hematuria_coagulos">
                            <div class="col-3"><label>¿Transfusión?</label><select name="vejiga_hematuria_transfusion"><option>NO</option><option>SI</option></select></div>
                        </div>
                        <div class="row">
                            <div class="col-2"><label>Procedimiento Qx</label>
                                <select name="vejiga_procedimiento_qx">
                                    <option>Ninguno</option>
                                    <option>RTU-V</option>
                                    <option>Cistoprostatectomía + Conducto Ileal</option>
                                    <option>Cistoprostatectomía + Ureterostomas</option>
                                    <option>Cistoprostatectomía + Neovejiga</option>
                                </select>
                            </div>
                            <div class="col-2"><label>Vía</label><select name="vejiga_via"><option>Abierta</option><option>Lap</option><option>Endo</option></select></div>
                            <div class="col-2"><label>RHP</label><input type="text" name="vejiga_rhp"></div>
                        </div>
                        <div class="row">
                            <div class="col-2"><label>Cistoscopias Previas</label><textarea rows="2" name="vejiga_cistoscopias_previas"></textarea></div>
                            <div class="col-2"><label>Quimio Intravesical</label><select name="vejiga_quimio_intravesical"><option>Ninguna</option><option>BCG</option><option>Mitomicina</option></select></div>
                            <div class="col-2"><label>Esquema/Dosis</label><input type="text" name="vejiga_esquema"></div>
                            <div class="col-2"><label>Tx Sistémico</label><input type="text" name="vejiga_sistemico"></div>
                        </div>
                    </div>

                    <div id="form_ca_prostata" class="dynamic-section">
                        <div class="section-title-internal">Protocolo Cáncer Próstata</div>
                        <div class="row">
                            <div class="col-4"><label>APE Pre-Bx (ng/mL)</label><input type="number" step="0.01" name="pros_ape_pre"></div>
                            <div class="col-4"><label>APE Actual</label><input type="number" step="0.01" name="pros_ape_act"></div>
                            <div class="col-4"><label>ECOG</label><input type="text" name="pros_ecog"></div>
                            <div class="col-4"><label>RMN (PIRADS/Zona)</label><input type="text" name="pros_rmn"></div>
                        </div>
                        <div class="row">
                            <div class="col-1"><label>Historial APE (Gráfica Recurrencia)</label><input type="text" name="pros_historial_ape" placeholder="Ingrese valores históricos para generar curva..."></div>
                        </div>
                        <div class="section-title-internal">Tacto Rectal (TNM Clínico)</div>
                        <div class="row">
                            <div class="col-4"><label>Tacto Rectal</label><input type="text" name="pros_tr"></div>
                            <div class="col-4"><label>Briganti (Nomograma)</label><input type="text" name="pros_briganti"></div>
                        </div>
                        <div class="row">
                            <div class="col-4"><label>Gleason</label><input type="text" name="pros_gleason"></div>
                            <div class="col-4"><label>TNM (T/N/M)</label><input type="text" name="pros_tnm"></div>
                            <div class="col-4"><label>Etapa / Riesgo</label><input type="text" name="pros_riesgo"></div>
                        </div>
                        <div class="row">
                            <div class="col-3"><label>ADT Previo</label><input type="text" name="pros_adt_previo" placeholder="¿Cuál?"></div>
                            <div class="col-3"><label>Prostatectomía</label><select name="pros_prostatectomia"><option>NO</option><option>Abierta</option><option>Lap</option></select></div>
                            <div class="col-3"><label>RHP (Factores Adversos)</label><input type="text" name="pros_rhp"></div>
                            <div class="col-3"><label>Radioterapia</label><input type="text" name="pros_radioterapia" placeholder="Ciclos/Dosis"></div>
                        </div>
                        <div class="row">
                            <div class="col-2"><label>Continencia</label><input type="text" name="pros_continencia"></div>
                            <div class="col-2"><label>Erección (Clasificación)</label><input type="text" name="pros_ereccion"></div>
                        </div>
                    </div>

                    <div id="form_ca_pene" class="dynamic-section">
                         <div class="section-title-internal">Protocolo Cáncer Pene</div>
                         <div class="row">
                             <div class="col-4"><label>Tiempo Dx / ECOG</label><input type="text" name="pene_tiempo_ecog"></div>
                             <div class="col-4"><label>TNM (Calcula Etapa)</label><input type="text" name="pene_tnm"></div>
                             <div class="col-4"><label>Tx Quirúrgico</label><select name="pene_tx_quirurgico"><option>NO</option><option>Penectomía Parcial</option><option>Total</option><option>Radical + Linfa</option></select></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>RHP</label><input type="text" name="pene_rhp"></div>
                             <div class="col-2"><label>Tx Sistémico</label><input type="text" name="pene_sistemico"></div>
                         </div>
                    </div>

                    <div id="form_ca_testiculo" class="dynamic-section">
                         <div class="section-title-internal">Protocolo Cáncer Testículo</div>
                         <div class="row">
                             <div class="col-4"><label>Tiempo Dx / ECOG</label><input type="text" name="testiculo_tiempo_ecog"></div>
                             <div class="col-4"><label>TNM (Calcula Etapa)</label><input type="text" name="testiculo_tnm"></div>
                             <div class="col-4"><label>Orquiectomía (Fecha)</label><input type="date" name="testiculo_orquiectomia_fecha"></div>
                         </div>
                         <div class="row">
                             <div class="col-3"><label>Marcadores PRE (AFP/HGC/DHL)</label><input type="text" name="testiculo_marcadores_pre"></div>
                             <div class="col-3"><label>Marcadores POST</label><input type="text" name="testiculo_marcadores_post"></div>
                             <div class="col-3"><label>RHP</label><input type="text" name="testiculo_rhp"></div>
                         </div>
                         <div class="row"><div class="col-1"><label>Historial Marcadores (Gráfica)</label><input type="text" name="testiculo_historial_marcadores"></div></div>
                    </div>

                    <div id="form_tumor_suprarrenal" class="dynamic-section">
                         <div class="section-title-internal">Protocolo Suprarrenal</div>
                         <div class="row">
                             <div class="col-3"><label>ECOG / Metanefrinas</label><input type="text" name="suprarrenal_ecog_metanefrinas"></div>
                             <div class="col-3"><label>Aldosterona / Cortisol</label><input type="text" name="suprarrenal_aldosterona_cortisol"></div>
                             <div class="col-3"><label>TNM / Etapa</label><input type="text" name="suprarrenal_tnm"></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>Tamaño (¿Aumento?)</label><input type="text" name="suprarrenal_tamano"></div>
                             <div class="col-2"><label>Cirugía</label><select name="suprarrenal_cirugia"><option>NO</option><option>Lap</option><option>Abierta</option></select></div>
                             <div class="col-2"><label>RHP</label><input type="text" name="suprarrenal_rhp"></div>
                         </div>
                    </div>

                    <div id="form_tumor_incierto" class="dynamic-section">
                         <div class="section-title-internal">Tumor Comportamiento Incierto</div>
                         <div class="row">
                             <div class="col-4"><label>APE / Densidad APE</label><input type="text" name="incierto_ape_densidad"></div>
                             <div class="col-4"><label>Tacto Rectal</label><input type="text" name="incierto_tr"></div>
                             <div class="col-4"><label>RMN (PIRADS)</label><input type="text" name="incierto_rmn"></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>Velocidad Replicación APE</label><input type="text" name="incierto_velocidad_ape"></div>
                             <div class="col-2"><label>% Necesidad BTR</label><input type="text" name="incierto_necesidad_btr"></div>
                         </div>
                    </div>

                    <div id="form_litiasis" class="dynamic-section" style="border-left-color: #2196F3;">
                         <div class="section-title-internal" style="color:#2196F3;">Protocolo Litiasis</div>
                         <div class="row" style="background:#e3f2fd; padding:10px;">
                             <div class="col-4"><label>Tamaño (mm)</label><input type="number" id="lit_tamano" name="lit_tamano" oninput="calcularScoresLitiasis()"></div>
                             <div class="col-4"><label>Localización</label>
                                <select id="lit_localizacion" name="lit_localizacion" onchange="calcularScoresLitiasis()">
                                    <option value="">Seleccionar</option><option value="renal_inf">Polo Inf</option><option value="coraliforme">Coraliforme</option><option value="ureter">Uréter</option><option value="otro">Otro</option>
                                </select>
                             </div>
                             <div class="col-4"><label>Densidad (UH)</label><input type="number" name="lit_densidad_uh"></div>
                         </div>
                         <div class="row">
                             <div class="col-3"><label>Estatus PostOp</label><select name="lit_estatus_postop"><option>Litiasis Residual</option><option>ZRF (Libre)</option></select></div>
                             <div class="col-3"><label>Unidad Metabólica</label><select name="lit_unidad_metabolica"><option>NO</option><option>SI (>20mm / Alto Riesgo)</option></select></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>Guy's Score</label><input type="text" id="guys_score" name="lit_guys_score" readonly class="calculated-field"></div>
                             <div class="col-2"><label>CROES Nomograma</label><input type="text" id="croes_score" name="lit_croes_score" readonly class="calculated-field"></div>
                         </div>
                    </div>

                    <div id="form_hpb" class="dynamic-section">
                         <div class="section-title-internal">Protocolo HPB</div>
                         <div class="row">
                             <div class="col-3"><label>Tamaño Próstata (cc)</label><input type="text" name="hpb_tamano_prostata"></div>
                             <div class="col-3"><label>APE</label><input type="text" name="hpb_ape"></div>
                             <div class="col-3"><label>IPSS</label><input type="text" name="hpb_ipss"></div>
                         </div>
                         <div class="row">
                             <div class="col-2"><label>Tamsulosina</label><input type="text" name="hpb_tamsulosina" placeholder="Dosis/Tiempo"></div>
                             <div class="col-2"><label>Finasteride/Dutasteride</label><input type="text" name="hpb_finasteride" placeholder="Dosis/Tiempo"></div>
                         </div>
                    </div>

                    <div id="form_otros" class="dynamic-section">
                         <div class="section-title-internal">Protocolo Específico</div>
                         <div class="row">
                             <div class="col-1"><label>Detalles Específicos (Pañales/día, Datos Donador, IPSS, Tamaño Próstata, etc.)</label><textarea rows="3" name="otro_detalles"></textarea></div>
                         </div>
                    </div>

                </fieldset>

                <fieldset>
                    <legend>8. Estudios Imagen/Lab/Gabinete</legend>
                    <div class="row"><div class="col-1"><label>Hallazgos Relevantes</label><textarea name="estudios_hallazgos" rows="3"></textarea></div></div>
                </fieldset>

                <fieldset style="background: var(--imss-dorado); color:white;">
                    <legend style="background:white; color:var(--imss-dorado);">9. Estatus del Protocolo</legend>
                    <div class="row">
                        <div class="col-1">
                            <label style="color:white; font-size:14px;">SELECCIONE DESTINO:</label>
                            <select name="estatus_protocolo" style="font-size:16px; padding:10px;">
                                <option value="incompleto">PROTOCOLO INCOMPLETO (Solicitar Estudios/Valoración)</option>
                                <option value="completo">PROTOCOLO COMPLETO -> LISTA DE ESPERA QUIRÚRGICA</option>
                                <option value="seguimiento">ACEPTADO PARA SEGUIMIENTO (Consulta)</option>
                            </select>
                        </div>
                    </div>
                    <div class="row">
                        <div class="col-1"><label style="color:white;">Especificaciones (Estudios faltantes o Procedimiento a programar)</label><input type="text" name="plan_especifico"></div>
                    </div>
                </fieldset>

                <button type="submit" class="save-btn">💾 GUARDAR EN EXPEDIENTE CLÍNICO ÚNICO</button>

            </div> <!-- fin seccion primera vez -->

            <div id="seccion_subsecuente">
                <fieldset>
                    <legend>Consulta Subsecuente - Resumen</legend>
                    <div class="row" style="background:#e8f5e9; padding:20px; border-radius:5px;">
                        <div class="col-1">
                            <h3>Resumen del Paciente (Simulado)</h3>
                            <p><strong>Paciente:</strong> (Carga datos previos...)</p>
                            <p><strong>Dx Principal:</strong> (Carga diagnóstico...)</p>
                            <p><strong>Última Nota:</strong> (Fecha y resumen...)</p>
                        </div>
                    </div>
                    <div class="section-title-internal">Nota de Evolución (SOAP)</div>
                    <div class="row">
                        <div class="col-1"><label>Subjetivo (S)</label><textarea rows="2" name="subsecuente_subjetivo"></textarea></div>
                        <div class="col-1"><label>Objetivo (O) - Inc. Labs/Imagen nuevos</label><textarea rows="2" name="subsecuente_objetivo"></textarea></div>
                        <div class="col-1"><label>Análisis (A)</label><textarea rows="2" name="subsecuente_analisis"></textarea></div>
                        <div class="col-1"><label>Plan (P)</label><textarea rows="2" name="subsecuente_plan"></textarea></div>
                    </div>
                    <div class="row">
                        <div class="col-2"><label>Actualizar RHP / Cirugías</label><input type="text" name="subsecuente_rhp_actualizar" placeholder="Si hubo nuevos eventos..."></div>
                    </div>
                    <button type="button" class="save-btn" onclick="alert('Nota Subsecuente Guardada')">💾 GUARDAR NOTA DE EVOLUCIÓN</button>
                </fieldset>
            </div>

        </form>
    </div>

    <script>
        function toggleConsultaTipo(tipo) {
            if(tipo === 'primera') {
                document.getElementById('seccion_primera_vez').style.display = 'block';
                document.getElementById('seccion_subsecuente').style.display = 'none';
                document.getElementById('btn-primera').classList.add('active');
                document.getElementById('btn-subsecuente').classList.remove('active');
            } else {
                document.getElementById('seccion_primera_vez').style.display = 'none';
                document.getElementById('seccion_subsecuente').style.display = 'block';
                document.getElementById('btn-primera').classList.remove('active');
                document.getElementById('btn-subsecuente').classList.add('active');
                generarNotaSOAP();
            }
        }

        function toggleDisplay(idTrigger, idTarget) {
            var val = document.getElementById(idTrigger).value.toLowerCase();
            var target = document.getElementById(idTarget);
            if(val === 'si' || val === 'positivo') target.style.display = 'block';
            else target.style.display = 'none';
        }

        function toggleAHF() {
            var val = document.getElementById('ahf_select').value;
            var target = document.getElementById('ahf_detalles');
            target.style.display = (val === 'si') ? 'block' : 'none';
        }

        function toggleForaneo() {
            var val = document.getElementById('select_alcaldia').value;
            var target = document.getElementById('div_foraneo');
            target.style.display = (val === 'foraneo') ? 'block' : 'none';
        }

        function calcularIMC() {
            var peso = parseFloat(document.getElementById('peso').value);
            var talla = parseFloat(document.getElementById('talla').value);
            var imcField = document.getElementById('imc');
            if(peso > 0 && talla > 50) {
                var imc = peso / ((talla/100) * (talla/100));
                imcField.value = imc.toFixed(2);
            } else {
                imcField.value = '';
            }
        }

        function calcularIT() {
            var cig = parseFloat(document.getElementById('cigarros_dia').value) || 0;
            var anios = parseFloat(document.getElementById('anios_fumando').value) || 0;
            var itField = document.getElementById('indice_tabaquico');
            if(cig > 0 && anios > 0) {
                var it = (cig * anios) / 20;
                itField.value = it.toFixed(1) + ' pq/año';
            } else {
                itField.value = '';
            }
        }

        function calcularScoresLitiasis() {
             var tamano = parseFloat(document.getElementById('lit_tamano').value) || 0;
             var loc = document.getElementById('lit_localizacion').value;
             var guys = "I"; var croes = "N/A";
             if(loc === 'coraliforme') guys = "IV";
             else if(loc === 'renal_inf') guys = "III";
             else if(tamano > 20) guys = "II";

             var baseCroes = 250 - (tamano*1.5);
             if(loc === 'coraliforme') baseCroes -= 80;
             if(baseCroes < 0) baseCroes = 0;
             croes = Math.round(baseCroes);

             document.getElementById('guys_score').value = "Grado " + guys;
             document.getElementById('croes_score').value = croes + " (Est)";
        }

        function generarNotaSOAP() {
            var subj = document.querySelector('textarea[name="subsecuente_subjetivo"]');
            var obj = document.querySelector('textarea[name="subsecuente_objetivo"]');
            var analisis = document.querySelector('textarea[name="subsecuente_analisis"]');
            var plan = document.querySelector('textarea[name="subsecuente_plan"]');

            if (!subj || !obj || !analisis || !plan) return;
            if (subj.value || obj.value || analisis.value || plan.value) return;

            var nombre = document.querySelector('input[name="nombre"]')?.value || 'Paciente';
            var diagnostico = document.querySelector('select[name="diagnostico_principal"]')?.value || 'diagnóstico no especificado';
            var imc = document.getElementById('imc')?.value || 'N/E';
            var planEsp = document.querySelector('input[name="plan_especifico"]')?.value || 'Plan pendiente de definir';
            var ta = document.querySelector('input[name="ta"]')?.value || 'N/E';
            var fc = document.querySelector('input[name="fc"]')?.value || 'N/E';
            var temp = document.querySelector('input[name="temp"]')?.value || 'N/E';
            var indice = document.getElementById('indice_tabaquico')?.value || 'N/E';
            var padecimiento = document.querySelector('textarea[name="padecimiento_actual"]')?.value || 'Sin síntomas referidos.';

            subj.value = nombre + " refiere: " + padecimiento;
            obj.value = "TA " + ta + ", FC " + fc + ", Temp " + temp + ". IMC " + imc + ". Índice tabáquico " + indice + ".";
            analisis.value = "Cuadro compatible con " + diagnostico + ". Evolución clínica en seguimiento.";
            plan.value = planEsp + ". Control y seguimiento según protocolo.";
        }

        function syncCoagulos() {
            var select = document.getElementById('vejiga_coagulos_tipo');
            var hidden = document.getElementById('vejiga_hematuria_coagulos');
            if (select && hidden) hidden.value = select.value;
        }

        function mostrarFormularioDinamico() {
            var diag = document.getElementById('diagnostico_principal').value;
            var sections = document.getElementsByClassName('dynamic-section');
            for(var i=0; i<sections.length; i++) sections[i].style.display = 'none';

            if(diag.startsWith('ca_rinon')) document.getElementById('form_ca_rinon').style.display = 'block';
            else if(diag.startsWith('ca_urotelial')) document.getElementById('form_ca_urotelial').style.display = 'block';
            else if(diag.startsWith('ca_vejiga')) { document.getElementById('form_ca_vejiga').style.display = 'block'; syncCoagulos(); }
            else if(diag.startsWith('ca_prostata')) document.getElementById('form_ca_prostata').style.display = 'block';
            else if(diag.startsWith('ca_pene')) document.getElementById('form_ca_pene').style.display = 'block';
            else if(diag.startsWith('ca_testiculo')) document.getElementById('form_ca_testiculo').style.display = 'block';
            else if(diag.startsWith('tumor_suprarrenal')) document.getElementById('form_tumor_suprarrenal').style.display = 'block';
            else if(diag.startsWith('tumor_incierto')) document.getElementById('form_tumor_incierto').style.display = 'block';
            else if(diag.startsWith('litiasis')) document.getElementById('form_litiasis').style.display = 'block';
            else if(diag.startsWith('hpb')) document.getElementById('form_hpb').style.display = 'block';
            else if(diag !== '') document.getElementById('form_otros').style.display = 'block';
        }
    </script>
</body>
</html>
"""

CONFIRMACION_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background-color: #f4f6f9; text-align: center; padding-top: 50px; margin: 0; }
        .card { background: white; max-width: 600px; margin: auto; padding: 50px; border-radius: 15px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); border-top: 8px solid #13322B; }
        h1 { color: #13322B; }
        h3 { color: #B38E5D; }
        a { display: inline-block; margin-top: 30px; padding: 15px 30px; background: #13322B; color: white; text-decoration: none; border-radius: 5px; }
    </style>
</head>
<body>
    <div class="card">
        <h1>REGISTRO GUARDADO</h1>
        <p><strong>PACIENTE:</strong> {{ nombre }}</p>
        <p><strong>DX:</strong> {{ diag }}</p>
        <h3>{{ msg_estatus }}</h3>
        {% if inconsistencias %}
        <div style="margin-top:20px; padding:15px; background:#fff8e1; border:1px solid #f0c36d; border-radius:10px; text-align:left;">
            <strong>Advertencias clínicas:</strong>
            <ul>
            {% for item in inconsistencias %}
                <li>{{ item }}</li>
            {% endfor %}
            </ul>
        </div>
        {% endif %}
        <br>
        <a href="/">INICIO</a>
    </div>
</body>
</html>
"""

# Plantillas para hospitalización, quirófano, búsqueda, expediente
HOSPITALIZACION_LISTA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Hospitalización - IMSS Urología</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; margin: 0; padding: 20px; }
        .container { max-width: 1200px; margin: auto; background: white; border-radius: 8px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); padding: 30px; }
        h1 { color: #13322B; border-bottom: 3px solid #B38E5D; padding-bottom: 10px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: #13322B; color: white; padding: 12px; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 20px; }
        .btn:hover { background: #0e2621; }
        .btn-dorado { background: #B38E5D; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏥 MÓDULO DE HOSPITALIZACIÓN</h1>
        <h2>Pacientes Hospitalizados</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>Paciente</th>
                <th>Ingreso</th>
                <th>Motivo</th>
                <th>Servicio</th>
                <th>Cama</th>
                <th>Estatus</th>
                <th>Acciones</th>
            </tr>
            {% for hosp in hospitalizaciones %}
            <tr>
                <td>{{ hosp.id }}</td>
                <td>{{ hosp.paciente_nombre }}</td>
                <td>{{ hosp.fecha_ingreso }}</td>
                <td>{{ hosp.motivo }}</td>
                <td>{{ hosp.servicio }}</td>
                <td>{{ hosp.cama }}</td>
                <td>{{ hosp.estatus }}</td>
                <td><a href="/expediente?consulta_id={{ hosp.consulta_id }}">Ver expediente</a></td>
            </tr>
            {% else %}
            <tr><td colspan="8" style="text-align:center;">No hay hospitalizaciones activas</td></tr>
            {% endfor %}
        </table>
        <a href="/hospitalizacion/nuevo" class="btn">➕ NUEVA HOSPITALIZACIÓN</a>
        <br><br>
        <a href="/" class="btn btn-dorado">← VOLVER AL MENÚ</a>
    </div>
</body>
</html>
"""

HOSPITALIZACION_NUEVO_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Nueva Hospitalización</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 600px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; }
        .form-group { margin-bottom: 20px; }
        label { font-weight: bold; display: block; margin-bottom: 5px; }
        input, select { width: 100%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        .btn { background: #13322B; color: white; padding: 12px 25px; border: none; border-radius: 5px; cursor: pointer; }
        .btn:hover { background: #0e2621; }
        .btn-dorado { background: #B38E5D; }
        .busqueda { display: flex; gap: 10px; }
        .busqueda input { flex: 1; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏥 Registrar Hospitalización</h1>
        <form action="/hospitalizacion/nuevo" method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
            <div class="form-group">
                <label>Buscar paciente por CURP o NSS</label>
                <div class="busqueda">
                    <input type="text" name="busqueda" placeholder="Ingrese CURP o NSS" required>
                    <button type="submit" formaction="/hospitalizacion/buscar" class="btn btn-dorado">Buscar</button>
                </div>
            </div>
            <div class="form-group">
                <label>Consulta ID</label>
                <input type="number" name="consulta_id" value="{{ consulta_id }}" readonly>
            </div>
            <div class="form-group">
                <label>Motivo de hospitalización</label>
                <input type="text" name="motivo" required>
            </div>
            <div class="form-group">
                <label>Servicio</label>
                <select name="servicio">
                    <option>Urología</option>
                    <option>Medicina Interna</option>
                    <option>Terapia Intensiva</option>
                    <option>Cirugía General</option>
                </select>
            </div>
            <div class="form-group">
                <label>Cama</label>
                <input type="text" name="cama" placeholder="Ej. 301-A">
            </div>
            <button type="submit" class="btn">Guardar Hospitalización</button>
        </form>
        <br>
        <a href="/hospitalizacion" class="btn btn-dorado">← Volver</a>
    </div>
</body>
</html>
"""

QUIROFANO_HOME_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Módulo Quirúrgico - IMSS Urología</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        :root { --imss-verde: #13322B; --imss-dorado: #B38E5D; --imss-fondo: #f3f6f5; }
        body { font-family: 'Montserrat', sans-serif; margin: 0; background: linear-gradient(120deg, #0f3a34, #1f5c53); min-height: 100vh; padding: 24px; }
        .container { max-width: 980px; margin: 0 auto; background: #fff; border-radius: 14px; border: 2px solid var(--imss-dorado); padding: 28px; box-shadow: 0 14px 34px rgba(0,0,0,0.28); }
        h1 { margin: 0 0 6px 0; text-align: center; color: var(--imss-verde); font-size: 34px; }
        h2 { margin: 0 0 24px 0; text-align: center; color: var(--imss-dorado); font-size: 19px; letter-spacing: 1px; text-transform: uppercase; }
        .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
        .card {
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            min-height: 220px; text-decoration: none; color: var(--imss-verde);
            background: var(--imss-fondo); border: 2px solid var(--imss-dorado); border-radius: 14px;
            transition: transform 0.18s ease, box-shadow 0.18s ease;
        }
        .card:hover { transform: translateY(-4px); box-shadow: 0 10px 22px rgba(19, 50, 43, 0.24); }
        .icon { font-size: 58px; line-height: 1; margin-bottom: 14px; }
        .label { font-size: 25px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.5px; text-align: center; }
        .actions { text-align: center; margin-top: 24px; }
        .btn-back { display: inline-block; text-decoration: none; background: var(--imss-verde); color: #fff; padding: 11px 20px; border-radius: 8px; font-weight: 700; }
        @media (max-width: 760px) {
            .grid { grid-template-columns: 1fr; }
            .card { min-height: 170px; }
            .label { font-size: 20px; }
            h1 { font-size: 28px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>MÓDULO QUIRÚRGICO</h1>
        <h2>Quirófano</h2>
        <div class="grid">
            <a class="card" href="/quirofano/urgencias">
                <div class="icon">🚨</div>
                <div class="label">Urgencias</div>
            </a>
            <a class="card" href="/quirofano/programada">
                <div class="icon">🗓️</div>
                <div class="label">Cirugía Programada</div>
            </a>
        </div>
        <div class="actions">
            <a class="btn-back" href="/">← Volver al menú principal</a>
        </div>
    </div>
</body>
</html>
"""

QUIROFANO_PROGRAMADA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Cirugía Programada - IMSS Urología</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        :root { --imss-verde: #13322B; --imss-dorado: #B38E5D; --imss-fondo: #f3f6f5; }
        body { font-family: 'Montserrat', sans-serif; margin: 0; background: linear-gradient(120deg, #0f3a34, #1f5c53); min-height: 100vh; padding: 24px; }
        .container { max-width: 1080px; margin: 0 auto; background: #fff; border-radius: 14px; border: 2px solid var(--imss-dorado); padding: 28px; box-shadow: 0 14px 34px rgba(0,0,0,0.28); }
        h1 { margin: 0 0 6px 0; text-align: center; color: var(--imss-verde); font-size: 34px; }
        h2 { margin: 0 0 24px 0; text-align: center; color: var(--imss-dorado); font-size: 19px; letter-spacing: 1px; text-transform: uppercase; }
        .grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 18px; }
        .card {
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            min-height: 210px; text-decoration: none; color: var(--imss-verde);
            background: var(--imss-fondo); border: 2px solid var(--imss-dorado); border-radius: 14px;
            transition: transform 0.18s ease, box-shadow 0.18s ease;
        }
        .card:hover { transform: translateY(-4px); box-shadow: 0 10px 22px rgba(19, 50, 43, 0.24); }
        .icon { font-size: 56px; line-height: 1; margin-bottom: 14px; }
        .label { font-size: 20px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.4px; text-align: center; padding: 0 8px; }
        .actions { text-align: center; margin-top: 24px; }
        .btn-back { display: inline-block; text-decoration: none; background: var(--imss-verde); color: #fff; padding: 11px 20px; border-radius: 8px; font-weight: 700; }
        @media (max-width: 920px) {
            .grid { grid-template-columns: 1fr; }
            .card { min-height: 170px; }
            .label { font-size: 18px; }
            h1 { font-size: 28px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>CIRUGÍA PROGRAMADA</h1>
        <h2>Módulo Quirúrgico</h2>
        <div class="grid">
            <a class="card" href="/quirofano/nuevo">
                <div class="icon">🗒️</div>
                <div class="label">Programar cirugía</div>
            </a>
            <a class="card" href="/quirofano/programada/lista">
                <div class="icon">✅</div>
                <div class="label">Cirugías programadas</div>
            </a>
            <a class="card" href="/quirofano/programada/postquirurgica">
                <div class="icon">📈</div>
                <div class="label">Realizar nota postquirúrgica</div>
            </a>
        </div>
        <div class="actions">
            <a class="btn-back" href="/quirofano">← Volver al módulo quirúrgico</a>
        </div>
    </div>
</body>
</html>
"""

QUIROFANO_PLACEHOLDER_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>{{ titulo }}</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        :root { --imss-verde: #13322B; --imss-dorado: #B38E5D; }
        body { font-family: 'Montserrat', sans-serif; margin: 0; background: #f4f6f9; padding: 24px; }
        .container { max-width: 860px; margin: 0 auto; background: #fff; border-radius: 12px; border-top: 5px solid var(--imss-verde); padding: 28px; box-shadow: 0 10px 22px rgba(0,0,0,0.12); }
        h1 { color: var(--imss-verde); margin-top: 0; }
        p { color: #35424a; line-height: 1.6; }
        .btn { display: inline-block; margin-top: 16px; text-decoration: none; background: var(--imss-verde); color: #fff; padding: 10px 18px; border-radius: 8px; font-weight: 700; }
        .btn-alt { background: var(--imss-dorado); margin-left: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>{{ titulo }}</h1>
        <p>{{ descripcion }}</p>
        <a class="btn" href="{{ return_url }}">← {{ return_label }}</a>
        <a class="btn btn-alt" href="/">Menú principal</a>
    </div>
</body>
</html>
"""

QUIROFANO_LISTA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Quirófano - IMSS Urología</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; margin: 0; padding: 20px; }
        .container { max-width: 1200px; margin: auto; background: white; border-radius: 8px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); padding: 30px; }
        h1 { color: #13322B; border-bottom: 3px solid #B38E5D; padding-bottom: 10px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: #13322B; color: white; padding: 12px; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 20px; }
        .btn:hover { background: #0e2621; }
        .btn-dorado { background: #B38E5D; }
        .filters { margin-top: 18px; display: grid; grid-template-columns: 220px 1fr 170px; gap: 10px; }
        .filters select, .filters input { padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        .filters button { background: #13322B; color: #fff; border: none; border-radius: 4px; cursor: pointer; font-weight: 700; }
        .small { color: #5c6670; font-size: 13px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔪 MÓDULO DE QUIRÓFANO</h1>
        <h2>Lista de Pacientes Programados</h2>
        <p class="small">Listado interconectado desde la base quirúrgica dedicada.</p>

        <form method="get" action="/quirofano/programada/lista" class="filters">
            <select name="campo">
                <option value="">Campo de búsqueda</option>
                <option value="nss" {% if campo == "nss" %}selected{% endif %}>NSS</option>
                <option value="paciente_nombre" {% if campo == "paciente_nombre" %}selected{% endif %}>Nombre</option>
                <option value="sexo" {% if campo == "sexo" %}selected{% endif %}>Sexo</option>
                <option value="patologia" {% if campo == "patologia" %}selected{% endif %}>Patología</option>
                <option value="procedimiento_programado" {% if campo == "procedimiento_programado" %}selected{% endif %}>Procedimiento</option>
                <option value="hgz" {% if campo == "hgz" %}selected{% endif %}>HGZ</option>
                <option value="estatus" {% if campo == "estatus" %}selected{% endif %}>Estatus</option>
            </select>
            <input type="text" name="q" value="{{ q or '' }}" placeholder="Valor a buscar...">
            <button type="submit">Buscar</button>
        </form>

        <table>
            <tr>
                <th>ID</th>
                <th>NSS</th>
                <th>Paciente</th>
                <th>Edad</th>
                <th>Sexo</th>
                <th>Patología</th>
                <th>Procedimiento</th>
                <th>Insumos</th>
                <th>HGZ</th>
                <th>Fecha Programada</th>
                <th>Estatus</th>
                <th>Acciones</th>
            </tr>
            {% for q in quirofanos %}
            <tr>
                <td>{{ q.id }}</td>
                <td>{{ q.nss or "-" }}</td>
                <td>{{ q.paciente_nombre }}</td>
                <td>{{ q.edad or "-" }}</td>
                <td>{{ q.sexo or "-" }}</td>
                <td>{{ q.patologia or "-" }}</td>
                <td>{{ q.procedimiento_programado or q.procedimiento }}</td>
                <td>{{ q.insumos_solicitados or "-" }}</td>
                <td>{{ q.hgz or "-" }}</td>
                <td>{{ q.fecha_programada }}</td>
                <td>{{ q.estatus }}</td>
                <td><a href="/expediente?consulta_id={{ q.consulta_id }}">Ver expediente</a></td>
            </tr>
            {% else %}
            <tr><td colspan="12" style="text-align:center;">No hay cirugías programadas</td></tr>
            {% endfor %}
        </table>
        <a href="/quirofano/nuevo" class="btn">➕ PROGRAMAR CIRUGÍA</a>
        <br><br>
        <a href="/quirofano/programada" class="btn btn-dorado">← VOLVER</a>
    </div>
</body>
</html>
"""

QUIROFANO_NUEVO_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Programar Cirugía</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        :root { --imss-verde: #13322B; --imss-dorado: #B38E5D; --gris: #f4f6f9; }
        body { font-family: 'Montserrat', sans-serif; background: var(--gris); padding: 20px; margin: 0; }
        .container { max-width: 1180px; margin: auto; background: white; border-radius: 10px; padding: 26px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); border-top: 6px solid var(--imss-verde); }
        h1 { color: var(--imss-verde); margin: 0 0 16px 0; }
        .form-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; }
        .form-group { margin-bottom: 14px; }
        .span-2 { grid-column: span 2; }
        .span-3 { grid-column: span 3; }
        label { font-weight: 700; display: block; margin-bottom: 6px; color: #24343a; font-size: 13px; text-transform: uppercase; }
        input, select, textarea { width: 100%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; font-family: 'Montserrat', sans-serif; }
        textarea { min-height: 80px; resize: vertical; }
        .section { margin-top: 20px; padding: 16px; border: 1px solid #e2e8f0; border-radius: 8px; background: #fafbfc; }
        .section h2 { margin: 0 0 14px 0; font-size: 18px; color: var(--imss-verde); }
        .section h3 { margin: 8px 0 10px 0; font-size: 14px; color: var(--imss-dorado); text-transform: uppercase; letter-spacing: 0.4px; }
        .hidden { display: none; }
        .insumos-wrap { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
        .insumo-item { border: 1px solid #d7dce2; border-radius: 6px; padding: 8px; background: #fff; }
        .insumo-item label { margin: 0; font-size: 12px; text-transform: none; cursor: pointer; }
        .insumo-item input { width: auto; margin-right: 6px; }
        .status-ok { color: #0b7a34; font-weight: 700; }
        .status-pending { color: #9a4a12; font-weight: 700; }
        .submit-bar {
            width: 100%;
            margin-top: 20px;
            background: linear-gradient(90deg, #13322B, #1f4d42);
            color: #fff;
            border: none;
            border-radius: 8px;
            padding: 14px 16px;
            font-size: 16px;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.7px;
            cursor: pointer;
        }
        .submit-bar .icon { margin-right: 10px; }
        .btn { background: #13322B; color: white; padding: 12px 25px; border: none; border-radius: 5px; cursor: pointer; text-decoration: none; display: inline-block; }
        .btn:hover { background: #0e2621; }
        .btn-dorado { background: #B38E5D; }
        @media (max-width: 980px) {
            .form-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .span-3 { grid-column: span 2; }
            .insumos-wrap { grid-template-columns: 1fr; }
        }
        @media (max-width: 640px) {
            .form-grid { grid-template-columns: 1fr; }
            .span-2, .span-3 { grid-column: span 1; }
            .container { padding: 16px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔪 Programar Cirugía</h1>
        <form action="/quirofano/nuevo" method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
            <div class="section">
                <h2>Datos obligatorios de programación</h2>
                <div class="form-grid">
                    <div class="form-group">
                        <label>Consulta ID</label>
                        <input type="number" id="consulta_id" name="consulta_id" required>
                    </div>
                    <div class="form-group">
                        <label>NSS</label>
                        <input type="text" id="nss" name="nss" required maxlength="10" pattern="\\d{10}" inputmode="numeric" placeholder="10 dígitos">
                    </div>
                    <div class="form-group">
                        <label>Agregado médico</label>
                        <input type="text" id="agregado_medico" name="agregado_medico" required>
                    </div>
                    <div class="form-group span-2">
                        <label>Nombre completo (apellidos primero)</label>
                        <input type="text" id="nombre_completo" name="nombre_completo" required placeholder="APELLIDO PATERNO APELLIDO MATERNO NOMBRES">
                    </div>
                    <div class="form-group">
                        <label>Edad</label>
                        <input type="number" id="edad" name="edad" min="0" max="120" required>
                    </div>
                    <div class="form-group">
                        <label>Sexo</label>
                        <select id="sexo" name="sexo" required>
                            <option value="">Seleccionar...</option>
                            {% for sexo in sexo_options %}
                            <option value="{{ sexo }}">{{ sexo }}</option>
                            {% endfor %}
                        </select>
                    </div>
                    <div class="form-group span-2">
                        <label>Patología</label>
                        <select id="patologia" name="patologia" required onchange="onPatologiaChange()">
                            <option value="">Seleccionar...</option>
                            {% for item in patologia_options %}
                            <option value="{{ item }}">{{ item }}</option>
                            {% endfor %}
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Fecha programada</label>
                        <input type="date" id="fecha_programada" name="fecha_programada" required>
                    </div>
                    <div class="form-group span-3">
                        <label>Procedimiento programado</label>
                        <select id="procedimiento_programado" name="procedimiento_programado" required onchange="onProcedimientoChange()">
                            <option value="">Seleccionar...</option>
                            {% for item in procedimiento_options %}
                            <option value="{{ item }}">{{ item }}</option>
                            {% endfor %}
                        </select>
                    </div>
                    <div class="form-group span-3">
                        <label>Insumos solicitados</label>
                        <div class="insumos-wrap">
                            {% for insumo in insumo_options %}
                            <div class="insumo-item">
                                <label><input type="checkbox" class="insumo-check" name="insumos_solicitados_list" value="{{ insumo }}" onchange="syncInsumos()"> {{ insumo }}</label>
                            </div>
                            {% endfor %}
                        </div>
                        <input type="hidden" id="insumos_solicitados" name="insumos_solicitados">
                    </div>
                    <div class="form-group">
                        <label>HGZ de procedencia</label>
                        <input type="text" id="hgz" name="hgz" required placeholder="Ej. HGZ 24">
                    </div>
                    <div class="form-group">
                        <label>Estatus</label>
                        <input type="text" id="estatus" name="estatus" value="PENDIENTE" readonly class="status-pending">
                    </div>
                </div>
            </div>

            <div id="onco_generico" class="section hidden">
                <h3>Ventana dinámica oncológica</h3>
                <div class="form-grid">
                    <div class="form-group"><label>TNM</label><input type="text" name="tnm"></div>
                    <div class="form-group"><label>ECOG</label><input type="text" name="ecog_onco"></div>
                    <div class="form-group"><label>Índice Charlson</label><input type="text" name="charlson"></div>
                    <div class="form-group"><label>Etapa clínica</label><input type="text" name="etapa_clinica"></div>
                </div>
            </div>

            <div id="onco_prostata" class="section hidden">
                <h3>Campos extra: Cáncer de próstata</h3>
                <div class="form-grid">
                    <div class="form-group"><label>IPSS</label><input type="text" name="ipss"></div>
                    <div class="form-group"><label>Gleason</label><input type="text" name="gleason"></div>
                    <div class="form-group"><label>APE</label><input type="text" name="ape"></div>
                    <div class="form-group"><label>RTUP previa</label>
                        <select name="rtup_previa"><option value="">Seleccionar...</option><option>SI</option><option>NO</option></select>
                    </div>
                </div>
            </div>

            <div id="incierto_prostata" class="section hidden">
                <h3>Campos: Tumor de comportamiento incierto próstata</h3>
                <div class="form-grid">
                    <div class="form-group"><label>Tacto rectal</label><input type="text" name="tacto_rectal"></div>
                    <div class="form-group"><label>Historial de APE</label><input type="text" name="historial_ape"></div>
                    <div class="form-group"><label>ECOG</label><input type="text" name="ecog_incierto"></div>
                </div>
            </div>

            <div id="litiasis_rinon" class="section hidden">
                <h3>Campos: Cálculo del riñón</h3>
                <div class="form-grid">
                    <div class="form-group">
                        <label>Unidades Hounsfield</label>
                        <select name="uh_rango">
                            <option value="">Seleccionar...</option>
                            <option>100 Y 500 UH</option>
                            <option>>500 A 1000 UH</option>
                            <option>>1000 Y 1200 UH</option>
                            <option>>1200 Y 1300 UH</option>
                            <option>>1300 Y 1400 UH</option>
                            <option>>1400 A 1500 UH</option>
                            <option>MAYOR A 1500 UH</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Tamaño</label>
                        <select id="litiasis_tamano_rango" name="litiasis_tamano_rango" onchange="onTamanoLitiasisChange()">
                            <option value="">Seleccionar...</option>
                            <option>&lt; 20 MM</option>
                            <option>&gt; 20 MM</option>
                        </select>
                    </div>
                    <div class="form-group hidden" id="litiasis_subtipo_wrap">
                        <label>Si &gt; 20 MM</label>
                        <select name="litiasis_subtipo_20">
                            <option value="">Seleccionar...</option>
                            <option>LITO CORAL COMPLETO</option>
                            <option>LITO CORAL INCOMPLETO</option>
                            <option>LITIASIS BILATERAL</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Ubicación</label>
                        <select id="litiasis_ubicacion" name="litiasis_ubicacion" onchange="onUbicacionLitiasisChange()">
                            <option value="">Seleccionar...</option>
                            <option>LITIASIS CALICIAL SUPERIOR</option>
                            <option>LITIASIS CALICIAL MEDIA</option>
                            <option>LITIASIS CALICIAL INFERIOR</option>
                            <option>LITIASIS PIELICA</option>
                            <option>LITIASIS CALICIAL MULTIPLE</option>
                        </select>
                    </div>
                    <div class="form-group hidden" id="litiasis_ubicacion_multiple_wrap">
                        <label>Especifique ubicaciones</label>
                        <input type="text" name="litiasis_ubicacion_multiple" placeholder="Ej. superior + inferior + pélvica">
                    </div>
                    <div class="form-group">
                        <label>Hidronefrosis</label>
                        <select name="hidronefrosis">
                            <option value="">Seleccionar...</option>
                            <option>SI</option>
                            <option>NO</option>
                        </select>
                    </div>
                </div>
            </div>

            <div id="procedimiento_dinamico" class="section">
                <h3>Ventana dinámica por procedimiento</h3>
                <div class="form-grid">
                    <div class="form-group hidden" id="tipo_neovejiga_wrap">
                        <label>Tipo de neovejiga</label>
                        <input type="text" name="tipo_neovejiga">
                    </div>
                    <div class="form-group hidden" id="sistema_succion_wrap">
                        <label>Sistema de succión</label>
                        <select name="sistema_succion">
                            <option value="">Seleccionar...</option>
                            <option>FANS</option>
                            <option>DISS</option>
                        </select>
                    </div>
                    <div class="form-group hidden" id="abordaje_wrap">
                        <label>Abordaje</label>
                        <select id="abordaje" name="abordaje">
                            <option value="">Seleccionar...</option>
                            <option>ABIERTO</option>
                            <option>LAPAROSCOPICO</option>
                            <option>ABIERTO + LAPAROSCOPICO</option>
                        </select>
                    </div>
                </div>
            </div>

            <button type="submit" class="submit-bar" onclick="return beforeSubmit()">
                <span class="icon">▮</span>Ingresar a lista de pacientes programados
            </button>
        </form>
        <br>
        <a href="/quirofano/programada" class="btn btn-dorado">← Volver</a>
    </div>
    <script>
        const patologiasOnco = new Set([{% for p in patologias_onco %}"{{ p }}",{% endfor %}]);
        const patologiasLitiasis = new Set([{% for p in patologias_litiasis %}"{{ p }}",{% endfor %}]);
        const procedimientosAbordaje = new Set([{% for p in procedimientos_abordaje %}"{{ p }}",{% endfor %}]);
        const procedimientosAbiertos = new Set([{% for p in procedimientos_abiertos %}"{{ p }}",{% endfor %}]);

        function byId(id) { return document.getElementById(id); }

        function show(id, visible) {
            const el = byId(id);
            if (!el) return;
            el.classList.toggle('hidden', !visible);
        }

        function syncInsumos() {
            const checks = Array.from(document.querySelectorAll('.insumo-check:checked')).map(x => x.value);
            byId('insumos_solicitados').value = checks.join(' | ');
            refreshEstatus();
        }

        function onPatologiaChange() {
            const pat = (byId('patologia').value || '').toUpperCase();
            show('onco_generico', patologiasOnco.has(pat));
            show('onco_prostata', pat === 'CANCER DE PROSTATA');
            show('incierto_prostata', pat === 'TUMOR DE COMPORTAMIENTO INCIERTO PROSTATA');
            show('litiasis_rinon', pat === 'CALCULO DEL RIÑON');
            refreshEstatus();
        }

        function onTamanoLitiasisChange() {
            const val = byId('litiasis_tamano_rango').value;
            show('litiasis_subtipo_wrap', val === '> 20 MM');
        }

        function onUbicacionLitiasisChange() {
            const val = byId('litiasis_ubicacion').value;
            show('litiasis_ubicacion_multiple_wrap', val === 'LITIASIS CALICIAL MULTIPLE');
        }

        function onProcedimientoChange() {
            const proc = (byId('procedimiento_programado').value || '').toUpperCase();
            show('tipo_neovejiga_wrap', proc === 'CISTOPROSTATECTOMIA RADICAL + FORMACION DE NEOVEJIGA');
            show('sistema_succion_wrap', proc === 'NEFROLITOTRICIA LASER FLEXIBLE CON SISTEMA DE SUCCION');

            const autoAbierto = procedimientosAbiertos.has(proc);
            const requiereAbordaje = procedimientosAbordaje.has(proc);
            show('abordaje_wrap', autoAbierto || requiereAbordaje);
            const abordaje = byId('abordaje');
            if (autoAbierto && abordaje) abordaje.value = 'ABIERTO';
            if (!autoAbierto && !requiereAbordaje && abordaje) abordaje.value = '';
            refreshEstatus();
        }

        function refreshEstatus() {
            const requiredIds = ['consulta_id', 'nss', 'agregado_medico', 'nombre_completo', 'edad', 'sexo', 'patologia', 'procedimiento_programado', 'hgz', 'fecha_programada'];
            let ok = true;
            for (const id of requiredIds) {
                const el = byId(id);
                if (!el || !String(el.value || '').trim()) { ok = false; break; }
            }
            if (!String(byId('insumos_solicitados').value || '').trim()) ok = false;
            const estatus = byId('estatus');
            estatus.value = ok ? 'PROGRAMADA' : 'PENDIENTE';
            estatus.className = ok ? 'status-ok' : 'status-pending';
        }

        function beforeSubmit() {
            syncInsumos();
            refreshEstatus();
            if (byId('estatus').value !== 'PROGRAMADA') {
                alert('Completa NSS, Agregado médico, Nombre completo, Edad, Sexo, Patología, Procedimiento programado, Insumos solicitados, HGZ y Fecha programada.');
                return false;
            }
            return true;
        }

        document.querySelectorAll('input, select, textarea').forEach(el => {
            el.addEventListener('input', refreshEstatus);
            el.addEventListener('change', refreshEstatus);
        });
        syncInsumos();
        onPatologiaChange();
        onProcedimientoChange();
        refreshEstatus();
    </script>
</body>
</html>
"""

EXPEDIENTE_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Expediente Clínico Único</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; margin: 0; padding: 20px; }
        .container { max-width: 1000px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; border-bottom: 3px solid #B38E5D; padding-bottom: 10px; }
        .section { margin-bottom: 30px; }
        .section h2 { color: #13322B; font-size: 18px; border-left: 5px solid #B38E5D; padding-left: 10px; }
        table { width: 100%; border-collapse: collapse; }
        td { padding: 8px; border-bottom: 1px solid #eee; }
        td:first-child { font-weight: bold; width: 30%; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📁 EXPEDIENTE CLÍNICO ÚNICO</h1>
        <div class="section">
            <h2>Datos del Paciente</h2>
            <table>
                <tr><td>Nombre:</td><td>{{ consulta.nombre }}</td></tr>
                <tr><td>CURP:</td><td>{{ consulta.curp }}</td></tr>
                <tr><td>NSS:</td><td>{{ consulta.nss }}</td></tr>
                <tr><td>Edad:</td><td>{{ consulta.edad }}</td></tr>
                <tr><td>Sexo:</td><td>{{ consulta.sexo }}</td></tr>
                <tr><td>Tipo de sangre:</td><td>{{ consulta.tipo_sangre }}</td></tr>
                <tr><td>Teléfono:</td><td>{{ consulta.telefono }}</td></tr>
                <tr><td>Email:</td><td>{{ consulta.email }}</td></tr>
                <tr><td>Dirección:</td><td>{{ consulta.calle }} {{ consulta.no_ext }}, {{ consulta.colonia }}, {{ consulta.alcaldia }}, CP {{ consulta.cp }}</td></tr>
            </table>
        </div>
        <div class="section">
            <h2>Diagnóstico Principal</h2>
            <p><strong>{{ consulta.diagnostico_principal }}</strong></p>
        </div>
        <div class="section">
            <h2>Detalles del Protocolo</h2>
            <pre>{{ protocolo_json }}</pre>
        </div>
        <div class="section">
            <h2>Estatus del Protocolo</h2>
            <p>{{ consulta.estatus_protocolo }} - {{ consulta.plan_especifico }}</p>
        </div>
        <div class="section">
            <h2>Archivos Clínicos Asociados</h2>
            {% if archivos_paciente %}
            <table>
                <tr><td style="font-weight:bold;">Seleccionar archivo:</td><td>
                    <select id="archivo_select">
                        {% for a in archivos_paciente %}
                        <option value="/archivos_paciente/{{ a.id }}">{{ a.nombre_original }} ({{ a.extension }})</option>
                        {% endfor %}
                    </select>
                    <button type="button" class="btn" style="margin-left:8px;" onclick="abrirArchivoSeleccionado()">Abrir</button>
                </td></tr>
            </table>
            <table style="margin-top:12px;">
                <tr><td>Nombre</td><td>Tamaño</td><td>Fecha</td><td>Acción</td></tr>
                {% for a in archivos_paciente %}
                <tr>
                    <td>{{ a.nombre_original }}</td>
                    <td>{{ a.tamano_legible }}</td>
                    <td>{{ a.fecha_subida }}</td>
                    <td><a href="/archivos_paciente/{{ a.id }}" target="_blank">Ver</a></td>
                </tr>
                {% endfor %}
            </table>
            {% else %}
            <p>Sin archivos asociados.</p>
            {% endif %}
            <a href="/analisis/cargar-archivos?consulta_id={{ consulta.id }}" class="btn">Cargar archivos</a>
        </div>
        <a href="/busqueda" class="btn">← Buscar otro paciente</a>
        <a href="/" class="btn">Menú Principal</a>
    </div>
    <script>
        function abrirArchivoSeleccionado() {
            var sel = document.getElementById('archivo_select');
            if (!sel || !sel.value) return;
            window.open(sel.value, '_blank');
        }
    </script>
</body>
</html>
"""

BUSQUEDA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Búsqueda de Pacientes</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 1000px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; }
        input[type=text] { width: 70%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        button { padding: 10px 20px; background: #13322B; color: white; border: none; border-radius: 4px; cursor: pointer; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: #13322B; color: white; padding: 12px; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        a { color: #13322B; text-decoration: none; font-weight: bold; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; }
        .btn-dorado { background: #B38E5D; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔎 Búsqueda de Pacientes</h1>
        <form method="get" action="/busqueda">
            <input type="text" name="q" placeholder="Ingrese CURP, NSS o nombre" value="{{ query }}">
            <button type="submit">Buscar</button>
        </form>
        {% if resultados %}
        <h2>Resultados:</h2>
        <table>
            <tr>
                <th>CURP</th>
                <th>Nombre</th>
                <th>Edad</th>
                <th>Diagnóstico</th>
                <th>Fecha</th>
                <th>Acciones</th>
            </tr>
            {% for r in resultados %}
            <tr>
                <td>{{ r.curp }}</td>
                <td>{{ r.nombre }}</td>
                <td>{{ r.edad }}</td>
                <td>{{ r.diagnostico_principal }}</td>
                <td>{{ r.fecha_registro }}</td>
                <td><a href="/expediente?consulta_id={{ r.id }}">Ver expediente</a></td>
            </tr>
            {% endfor %}
        </table>
        {% elif query %}
        <p>No se encontraron resultados para "{{ query }}".</p>
        {% endif %}
        <br><br>
        <a href="/busqueda_semantica" class="btn">Búsqueda Semántica</a>
        <br><br>
        <a href="/" class="btn btn-dorado">← Volver al Menú</a>
    </div>
</body>
</html>
"""

BUSQUEDA_SEMANTICA_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Búsqueda Semántica</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 1000px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; }
        input[type=text] { width: 70%; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        button { padding: 10px 20px; background: #13322B; color: white; border: none; border-radius: 4px; cursor: pointer; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: #13322B; color: white; padding: 12px; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        a { color: #13322B; text-decoration: none; font-weight: bold; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; }
        .btn-dorado { background: #B38E5D; }
        .notice { margin-top: 15px; padding: 12px; background: #fff8e1; border-radius: 6px; border: 1px solid #f0c36d; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔎 Búsqueda Semántica</h1>
        <form method="get" action="/busqueda_semantica">
            <input type="text" name="q" placeholder="Ej. dolor lumbar hematuria" value="{{ query }}">
            <button type="submit">Buscar</button>
        </form>
        {% if message %}
        <div class="notice">{{ message }}</div>
        {% endif %}
        {% if resultados %}
        <h2>Resultados:</h2>
        <table>
            <tr>
                <th>CURP</th>
                <th>Nombre</th>
                <th>Diagnóstico</th>
                <th>Similitud</th>
                <th>Acciones</th>
            </tr>
            {% for r in resultados %}
            <tr>
                <td>{{ r.curp }}</td>
                <td>{{ r.nombre }}</td>
                <td>{{ r.diagnostico_principal }}</td>
                <td>{{ r.similitud }}</td>
                <td><a href="/expediente?consulta_id={{ r.id }}">Ver expediente</a></td>
            </tr>
            {% endfor %}
        </table>
        {% endif %}
        <br><br>
        <a href="/" class="btn btn-dorado">← Volver al Menú</a>
    </div>
</body>
</html>
"""

REPORTE_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reporte BI - Urología</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; padding: 20px; }
        .container { max-width: 1200px; margin: auto; background: white; border-radius: 8px; padding: 30px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        h1 { color: #13322B; margin-bottom: 10px; }
        h2 { color: #13322B; margin-top: 30px; }
        .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; }
        .card { background: #f9fafb; padding: 15px; border-radius: 8px; border: 1px solid #e5e7eb; }
        img { max-width: 100%; border-radius: 8px; border: 1px solid #e5e7eb; }
        .notice { padding: 12px; background: #fff8e1; border-radius: 6px; border: 1px solid #f0c36d; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .btn { background: #13322B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        table th { background: #13322B; color: #fff; padding: 8px; font-size: 13px; text-align: left; }
        table td { border-bottom: 1px solid #e6e6e6; padding: 8px; font-size: 13px; }
        .split { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 18px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Reporte BI - Expediente Clínico Único</h1>
        <p>Generado: {{ fecha }}</p>
        <a href="/analisis/cargar-archivos" class="btn" style="margin-bottom:14px;">Cargar archivos</a>

        <div class="summary">
            <div class="card"><strong>Total pacientes:</strong><br>{{ total }}</div>
            <div class="card"><strong>Oncológicos:</strong><br>{{ total_onco }}</div>
            <div class="card"><strong>Protocolos completos:</strong><br>{{ completos }}</div>
            <div class="card"><strong>Protocolos incompletos:</strong><br>{{ incompletos }}</div>
            <div class="card"><strong>Pacientes programados:</strong><br>{{ total_programados }}</div>
        </div>

        {% if notice %}
        <div class="notice" style="margin-top:20px;">{{ notice }}</div>
        {% endif %}

        {% if chart_diagnosticos %}
        <h2>Distribución de Diagnósticos</h2>
        <img src="data:image/png;base64,{{ chart_diagnosticos }}" alt="Distribución diagnósticos">
        {% endif %}

        {% if numeric_charts %}
        <h2>Variables Numéricas</h2>
        <div class="grid">
            {% for chart in numeric_charts %}
            <div>
                <img src="data:image/png;base64,{{ chart }}" alt="Histograma">
            </div>
            {% endfor %}
        </div>
        {% endif %}

        {% if chart_survival %}
        <h2>Curva de Supervivencia (Kaplan-Meier)</h2>
        <img src="data:image/png;base64,{{ chart_survival }}" alt="Kaplan-Meier">
        <p style="font-size:12px; color:#555;">Evento aproximado: estatus_protocolo = completo. Censura para otros estados.</p>
        {% endif %}

        {% if chart_waitlist %}
        <h2>Predicción Lista de Espera Quirúrgica</h2>
        <img src="data:image/png;base64,{{ chart_waitlist }}" alt="Proyección lista de espera">
        {% endif %}

        <h2>Programación Quirúrgica: Sexo y Patología</h2>
        <div class="split">
            <div>
                <h3>Pacientes programados por sexo</h3>
                <table>
                    <tr><th>Sexo</th><th>Cantidad</th></tr>
                    {% for item, count in sexo_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Pacientes programados por patología</h3>
                <table>
                    <tr><th>Patología</th><th>Cantidad</th></tr>
                    {% for item, count in patologias_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
        </div>

        <h2>Pacientes Oncológicos Programados</h2>
        <div class="split">
            <div>
                <h3>Desglose por diagnóstico</h3>
                <table>
                    <tr><th>Diagnóstico</th><th>Cantidad</th></tr>
                    {% for item, count in onco_diag_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Desglose por ECOG</h3>
                <table>
                    <tr><th>ECOG</th><th>Cantidad</th></tr>
                    {% for item, count in onco_ecog_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Desglose por Charlson</h3>
                <table>
                    <tr><th>Charlson</th><th>Cantidad</th></tr>
                    {% for item, count in onco_charlson_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Desglose por edad</h3>
                <table>
                    <tr><th>Rango</th><th>Cantidad</th></tr>
                    {% for item, count in onco_edad_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
        </div>
        {% if onco_pacientes %}
        <h3>Listado oncológico programado</h3>
        <table>
            <tr><th>NSS</th><th>Paciente</th><th>Edad</th><th>Patología</th><th>ECOG</th><th>Charlson</th></tr>
            {% for p in onco_pacientes %}
            <tr>
                <td>{{ p.nss }}</td>
                <td>{{ p.nombre }}</td>
                <td>{{ p.edad }}</td>
                <td>{{ p.patologia }}</td>
                <td>{{ p.ecog }}</td>
                <td>{{ p.charlson }}</td>
            </tr>
            {% endfor %}
        </table>
        {% endif %}

        <h2>Pacientes con Litiasis Urinaria Programados</h2>
        <div class="split">
            <div>
                <h3>Por diagnóstico</h3>
                <table><tr><th>Diagnóstico</th><th>Cantidad</th></tr>
                    {% for item, count in litiasis_diag_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Cálculo renal por UH</h3>
                <table><tr><th>UH</th><th>Cantidad</th></tr>
                    {% for item, count in litiasis_uh_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Cálculo renal por tamaño</h3>
                <table><tr><th>Tamaño</th><th>Cantidad</th></tr>
                    {% for item, count in litiasis_tamano_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Subtipo &gt;20 mm</h3>
                <table><tr><th>Subtipo</th><th>Cantidad</th></tr>
                    {% for item, count in litiasis_subtipo_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Ubicación litiasis</h3>
                <table><tr><th>Ubicación</th><th>Cantidad</th></tr>
                    {% for item, count in litiasis_ubicacion_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Hidronefrosis</h3>
                <table><tr><th>Estado</th><th>Cantidad</th></tr>
                    {% for item, count in litiasis_hidronefrosis_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
        </div>

        <h2>Procedimientos Programados</h2>
        <div class="split">
            <div>
                <h3>Conteo por procedimiento</h3>
                <table><tr><th>Procedimiento</th><th>Cantidad</th></tr>
                    {% for item, count in procedimientos_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Abordaje + procedimiento</h3>
                <table><tr><th>Abordaje | Procedimiento</th><th>Cantidad</th></tr>
                    {% for item, count in procedimiento_abordaje_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Sistemas de succión (FANS/DISS)</h3>
                <table><tr><th>Sistema</th><th>Cantidad</th></tr>
                    {% for item, count in succion_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Pacientes con equipos integrales (INTERMED)</h3>
                <table><tr><th>Procedimiento</th><th>Cantidad</th></tr>
                    {% for item, count in intermed_por_procedimiento %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                <h3>Programados por HGZ</h3>
                <table><tr><th>HGZ</th><th>Cantidad</th></tr>
                    {% for item, count in hgz_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
        </div>

        <h2>Programados por Edad</h2>
        <div class="split">
            <div>
                <h3>Lista alimentada por edad</h3>
                <table>
                    <tr><th>Rango/Edad</th><th>Cantidad</th></tr>
                    {% for item, count in edad_programados_counts %}
                    <tr><td>{{ item }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>
            <div>
                {% if chart_edad_combinada %}
                <h3>Promedio por edad integrado</h3>
                <img src="data:image/png;base64,{{ chart_edad_combinada }}" alt="Promedio por edad integrado">
                {% endif %}
            </div>
        </div>

        <h2>Desglose combinado (Edad + Diagnóstico + Procedimiento + ECOG + Sexo)</h2>
        <table>
            <tr>
                <th>Edad</th>
                <th>Diagnóstico</th>
                <th>Procedimiento</th>
                <th>ECOG</th>
                <th>Sexo</th>
                <th>Cantidad</th>
            </tr>
            {% for row in edad_combinado_counts %}
            <tr>
                <td>{{ row.edad }}</td>
                <td>{{ row.diagnostico }}</td>
                <td>{{ row.procedimiento }}</td>
                <td>{{ row.ecog }}</td>
                <td>{{ row.sexo }}</td>
                <td>{{ row.cantidad }}</td>
            </tr>
            {% endfor %}
        </table>

        <br>
        <a href="/" class="btn">← Volver al Menú</a>
    </div>
</body>
</html>
"""

DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Dashboard Epidemiológico - Urología IMSS</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="/static/vendor/vue.min.js"></script>
    <script src="/static/vendor/axios.min.js"></script>
    <script src="/static/vendor/chart.min.js"></script>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        :root { --imss-verde: #13322B; --imss-dorado: #B38E5D; --fondo: #f4f6f9; }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Montserrat', sans-serif; background: var(--fondo); padding: 20px; }
        .container { max-width: 1400px; margin: auto; background: white; border-radius: 12px; padding: 25px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }
        h1 { color: var(--imss-verde); border-bottom: 3px solid var(--imss-dorado); padding-bottom: 10px; margin-bottom: 25px; }
        .filters { display: flex; gap: 15px; margin-bottom: 25px; flex-wrap: wrap; }
        .filter-item { background: #f8f9fa; padding: 15px; border-radius: 8px; flex: 1; min-width: 200px; }
        .filter-item label { font-weight: 700; display: block; margin-bottom: 5px; color: var(--imss-verde); }
        select, input { width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px; }
        .stat-card { background: white; border: 1px solid #e0e0e0; border-left: 6px solid var(--imss-dorado); padding: 20px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.05); }
        .stat-card h3 { font-size: 14px; color: #666; margin-bottom: 5px; }
        .stat-card .value { font-size: 32px; font-weight: 800; color: var(--imss-verde); }
        .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(450px, 1fr)); gap: 25px; margin-bottom: 30px; }
        .chart-container { background: white; padding: 15px; border-radius: 8px; border: 1px solid #eee; min-height: 360px; }
        canvas { max-height: 300px; width: 100% !important; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th { background: var(--imss-verde); color: white; padding: 10px; }
        td { padding: 8px; border-bottom: 1px solid #ddd; }
        .loading { text-align: center; padding: 40px; color: var(--imss-verde); }
        .toolbar { margin-bottom: 20px; display: flex; gap: 10px; align-items: center; }
        .btn { background: var(--imss-verde); color: #fff; border: none; border-radius: 6px; padding: 10px 14px; cursor: pointer; }
        .empty { color: #666; font-size: 13px; margin-top: 8px; }
    </style>
</head>
<body>
    <div id="app" class="container">
        <h1>DASHBOARD EPIDEMIOLOGICO - UROLOGIA</h1>
        <div class="toolbar">
            <button class="btn" @click="cargarDatos">Actualizar</button>
            <a href="/analisis/cargar-archivos" class="btn" style="text-decoration:none;">Cargar archivos</a>
            <a href="/" class="btn" style="text-decoration:none;">Menu principal</a>
        </div>

        <div class="filters">
            <div class="filter-item">
                <label>Año</label>
                <select v-model="filtroAnio" @change="cargarDatos">
                    <option value="">Todos</option>
                    <option v-for="a in years" :key="'y-'+a" :value="a">{{ a }}</option>
                </select>
            </div>
            <div class="filter-item">
                <label>Mes</label>
                <select v-model="filtroMes" @change="cargarDatos">
                    <option value="">Todos</option>
                    <option value="1">Enero</option><option value="2">Febrero</option><option value="3">Marzo</option>
                    <option value="4">Abril</option><option value="5">Mayo</option><option value="6">Junio</option>
                    <option value="7">Julio</option><option value="8">Agosto</option><option value="9">Septiembre</option>
                    <option value="10">Octubre</option><option value="11">Noviembre</option><option value="12">Diciembre</option>
                </select>
            </div>
            <div class="filter-item">
                <label>Diagnostico</label>
                <select v-model="filtroDiagnostico" @change="cargarDatos">
                    <option value="">Todos</option>
                    <option v-for="d in diagnosticos" :key="'d-'+d" :value="d">{{ d }}</option>
                </select>
            </div>
            <div class="filter-item">
                <label>HGZ</label>
                <select v-model="filtroHGZ" @change="cargarDatos">
                    <option value="">Todos</option>
                    <option v-for="h in hgzList" :key="'h-'+h" :value="h">{{ h }}</option>
                </select>
            </div>
        </div>

        <div v-if="loading" class="loading">Cargando datos...</div>

        <div v-else>
            <div class="stats-grid">
                <div class="stat-card"><h3>PACIENTES PROGRAMADOS</h3><div class="value">{{ stats.total }}</div></div>
                <div class="stat-card"><h3>ONCOLOGICOS</h3><div class="value">{{ stats.oncologicos }}</div></div>
                <div class="stat-card"><h3>LITIASIS</h3><div class="value">{{ stats.litiasis }}</div></div>
                <div class="stat-card"><h3>PROCEDIMIENTOS UNICOS</h3><div class="value">{{ stats.proc_unicos }}</div></div>
            </div>

            <div class="charts">
                <div class="chart-container">
                    <h3>Tendencia de Programaciones</h3>
                    <canvas id="chartTendencia"></canvas>
                    <div v-if="!tendenciaData.length" class="empty">Sin datos para el filtro seleccionado.</div>
                </div>
                <div class="chart-container">
                    <h3>Distribucion por Diagnostico</h3>
                    <canvas id="chartDiagnostico"></canvas>
                </div>
                <div class="chart-container">
                    <h3>Distribucion por Sexo</h3>
                    <canvas id="chartSexo"></canvas>
                </div>
                <div class="chart-container">
                    <h3>Top 10 Procedimientos</h3>
                    <canvas id="chartProcedimientos"></canvas>
                </div>
            </div>

            <h2>Desglose Detallado</h2>
            <table>
                <thead>
                    <tr>
                        <th>Fecha</th>
                        <th>Diagnostico</th>
                        <th>Procedimiento</th>
                        <th>Sexo</th>
                        <th>Edad</th>
                        <th>HGZ</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="row in detalles" :key="'row-'+row.id">
                        <td>{{ row.fecha }}</td>
                        <td>{{ row.diagnostico }}</td>
                        <td>{{ row.procedimiento }}</td>
                        <td>{{ row.sexo }}</td>
                        <td>{{ row.edad }}</td>
                        <td>{{ row.hgz }}</td>
                    </tr>
                    <tr v-if="!detalles.length">
                        <td colspan="6" style="text-align:center;">Sin registros para el filtro seleccionado.</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        new Vue({
            el: '#app',
            data: {
                loading: true,
                filtroAnio: '',
                filtroMes: '',
                filtroDiagnostico: '',
                filtroHGZ: '',
                years: [],
                diagnosticos: [],
                hgzList: [],
                stats: { total: 0, oncologicos: 0, litiasis: 0, proc_unicos: 0 },
                tendenciaData: [],
                detalles: [],
                charts: {}
            },
            mounted() {
                this.cargarCatalogos();
                this.cargarDatos();
            },
            methods: {
                buildParams(includeHGZ = true) {
                    const p = {};
                    if (this.filtroAnio) p.anio = Number(this.filtroAnio);
                    if (this.filtroMes) p.mes = Number(this.filtroMes);
                    if (this.filtroDiagnostico) p.diagnostico = this.filtroDiagnostico;
                    if (includeHGZ && this.filtroHGZ) p.hgz = this.filtroHGZ;
                    return p;
                },
                async cargarCatalogos() {
                    try {
                        const [catRes, hgzRes] = await Promise.all([
                            axios.get('/qx/catalogos_cached'),
                            axios.get('/api/dashboard/hgz')
                        ]);
                        this.diagnosticos = catRes.data.patologias || [];
                        this.hgzList = (hgzRes.data || []).map(x => x.hgz);
                    } catch(e) {
                        console.error(e);
                    }
                },
                async cargarDatos() {
                    this.loading = true;
                    try {
                        const params = this.buildParams(true);
                        const paramsNoHGZ = this.buildParams(false);
                        const [resumenRes, tendenciaRes, diagRes, sexoRes, procRes, detalleRes, hgzRes] = await Promise.all([
                            axios.get('/api/dashboard/resumen', { params }),
                            axios.get('/api/dashboard/tendencia', { params }),
                            axios.get('/api/dashboard/diagnosticos', { params }),
                            axios.get('/api/dashboard/sexo', { params }),
                            axios.get('/api/dashboard/procedimientos_top', { params }),
                            axios.get('/api/dashboard/detalle', { params }),
                            axios.get('/api/dashboard/hgz', { params: paramsNoHGZ }),
                        ]);

                        this.stats = resumenRes.data || this.stats;
                        this.tendenciaData = tendenciaRes.data || [];
                        this.detalles = detalleRes.data || [];
                        this.hgzList = (hgzRes.data || []).map(x => x.hgz);

                        const years = [...new Set((this.tendenciaData || []).map(d => d.anio))].filter(v => !!v);
                        this.years = years.sort((a,b) => b - a);

                        this.$nextTick(() => {
                            this.destroyCharts();
                            this.renderCharts(this.tendenciaData, diagRes.data || [], sexoRes.data || [], procRes.data || []);
                        });
                    } catch(e) {
                        console.error(e);
                        alert('Error al cargar datos del dashboard');
                    } finally {
                        this.loading = false;
                    }
                },
                renderCharts(tendencia, diagnosticos, sexo, procedimientos) {
                    // Tendencia
                    const ctx1 = document.getElementById('chartTendencia').getContext('2d');
                    this.charts.tendencia = new Chart(ctx1, {
                        type: 'line',
                        data: {
                            labels: tendencia.map(d => `${d.anio}-${String(d.mes).padStart(2,'0')}`),
                            datasets: [{
                                label: 'Programaciones',
                                data: tendencia.map(d => d.cantidad),
                                borderColor: '#13322B',
                                backgroundColor: 'rgba(19,50,43,0.10)',
                                tension: 0.2
                            }]
                        },
                        options: { responsive: true, maintainAspectRatio: false }
                    });

                    // Diagnosticos
                    const ctx2 = document.getElementById('chartDiagnostico').getContext('2d');
                    this.charts.diagnostico = new Chart(ctx2, {
                        type: 'bar',
                        data: {
                            labels: diagnosticos.map(d => d.label),
                            datasets: [{ label: 'Casos', data: diagnosticos.map(d => d.value), backgroundColor: '#B38E5D' }]
                        },
                        options: { responsive: true, maintainAspectRatio: false }
                    });

                    // Sexo
                    const ctx3 = document.getElementById('chartSexo').getContext('2d');
                    this.charts.sexo = new Chart(ctx3, {
                        type: 'doughnut',
                        data: {
                            labels: sexo.map(d => d.label),
                            datasets: [{
                                data: sexo.map(d => d.value),
                                backgroundColor: ['#13322B', '#B38E5D', '#7FA59A']
                            }]
                        },
                        options: { responsive: true, maintainAspectRatio: false }
                    });

                    // Procedimientos
                    const ctx4 = document.getElementById('chartProcedimientos').getContext('2d');
                    this.charts.procedimientos = new Chart(ctx4, {
                        type: 'bar',
                        data: {
                            labels: procedimientos.map(d => d.label),
                            datasets: [{ label: 'Cantidad', data: procedimientos.map(d => d.value), backgroundColor: '#13322B' }]
                        },
                        options: { responsive: true, maintainAspectRatio: false }
                    });
                },
                destroyCharts() {
                    Object.values(this.charts).forEach(c => c && c.destroy());
                    this.charts = {};
                }
            }
        });
    </script>
</body>
</html>
"""


CARGA_ARCHIVOS_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Cargar Archivos - Análisis de Datos</title>
    <link href="/static/css/fonts_offline.css" rel="stylesheet">
    <style>
        :root { --imss-verde: #13322B; --imss-dorado: #B38E5D; }
        body { font-family: 'Montserrat', sans-serif; background: #f4f6f9; margin: 0; padding: 20px; }
        .container { max-width: 1100px; margin: auto; background: #fff; border-radius: 10px; padding: 26px; box-shadow: 0 8px 24px rgba(0,0,0,0.08); }
        h1 { color: var(--imss-verde); margin-bottom: 10px; }
        h2 { color: var(--imss-verde); margin-top: 24px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin-top: 14px; }
        .card { border: 1px solid #e6e6e6; border-radius: 8px; padding: 14px; background: #fafafa; }
        label { font-size: 12px; color: #354; font-weight: 700; display: block; margin-bottom: 6px; text-transform: uppercase; }
        input, textarea { width: 100%; padding: 10px; border: 1px solid #ccc; border-radius: 6px; font-family: inherit; }
        .btn { display: inline-block; margin-top: 12px; border: none; border-radius: 6px; padding: 10px 16px; background: var(--imss-verde); color: #fff; text-decoration: none; cursor: pointer; }
        .btn-secondary { background: var(--imss-dorado); color: #1d1d1d; }
        table { width: 100%; border-collapse: collapse; margin-top: 14px; }
        th { background: var(--imss-verde); color: #fff; padding: 10px; font-size: 12px; text-align: left; }
        td { padding: 10px; border-bottom: 1px solid #eee; font-size: 13px; }
        .alert { margin-top: 12px; border-radius: 6px; padding: 10px 12px; }
        .ok { background: #e8f5e9; color: #1b5e20; border: 1px solid #c8e6c9; }
        .err { background: #fdecea; color: #8a1c1c; border: 1px solid #f5c6cb; }
        .hint { color: #555; font-size: 12px; margin-top: 6px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📂 Cargar Archivos</h1>
        <p>Módulo de análisis de datos. Suba archivos clínicos y asócielos a un paciente por <strong>Consulta ID</strong> o <strong>CURP</strong>.</p>

        {% if message %}
        <div class="alert ok">{{ message }}</div>
        {% endif %}
        {% if error %}
        <div class="alert err">{{ error }}</div>
        {% endif %}
        {% if errores and errores|length %}
        <div class="alert err">
            {% for e in errores %}
            <div>{{ e }}</div>
            {% endfor %}
        </div>
        {% endif %}

        <div class="card">
            <h2>Adjuntar a paciente</h2>
            <form action="/analisis/cargar-archivos" method="post" enctype="multipart/form-data">
                <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
                <div class="grid">
                    <div>
                        <label>Consulta ID</label>
                        <input type="number" name="consulta_id" value="{{ consulta_id or '' }}" placeholder="Ej. 1254">
                    </div>
                    <div>
                        <label>CURP (alternativo)</label>
                        <input type="text" name="curp" value="{{ curp or '' }}" placeholder="Ej. ABCD001122HDFXXX09">
                    </div>
                    <div style="grid-column: 1 / -1;">
                        <label>Descripción</label>
                        <textarea name="descripcion" rows="2" placeholder="Descripción corta del archivo (opcional)"></textarea>
                    </div>
                    <div style="grid-column: 1 / -1;">
                        <label>Archivos (Excel, DOC, PDF, PNG/PGN, DICOM)</label>
                        <input type="file" name="files" multiple accept=".xlsx,.xls,.doc,.docx,.pdf,.png,.pgn,.dcm,.dicom" required>
                        <div class="hint">Tamaño máximo por archivo: {{ max_size_mb }} MB.</div>
                    </div>
                </div>
                <button class="btn" type="submit">Guardar archivos</button>
            </form>
        </div>

        <div class="card" style="margin-top: 18px;">
            <h2>Carga masiva de pacientes (Excel)</h2>
            <form action="/carga_masiva_excel" method="post" enctype="multipart/form-data">
                <input type="hidden" name="csrf_token" value="{{ csrf_token }}">
                <label>Archivo Excel (.xlsx/.xls)</label>
                <input type="file" name="file" accept=".xlsx,.xls" required>
                <button class="btn btn-secondary" type="submit">Procesar carga masiva</button>
            </form>
        </div>

        {% if consulta %}
        <div class="card" style="margin-top: 18px;">
            <h2>Paciente seleccionado</h2>
            <p><strong>ID:</strong> {{ consulta.id }} &nbsp; <strong>Nombre:</strong> {{ consulta.nombre }} &nbsp; <strong>CURP:</strong> {{ consulta.curp }}</p>
            <a href="/expediente?consulta_id={{ consulta.id }}" class="btn">Ver expediente</a>
        </div>
        {% endif %}

        <div class="card" style="margin-top: 18px;">
            <h2>Archivos asociados</h2>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Consulta</th>
                        <th>Nombre</th>
                        <th>Tipo</th>
                        <th>Tamaño</th>
                        <th>Fecha</th>
                        <th>Acción</th>
                    </tr>
                </thead>
                <tbody>
                    {% for a in archivos_paciente %}
                    <tr>
                        <td>{{ a.id }}</td>
                        <td>{{ a.consulta_id }}</td>
                        <td>{{ a.nombre_original }}</td>
                        <td>{{ a.extension }}</td>
                        <td>{{ a.tamano_legible }}</td>
                        <td>{{ a.fecha_subida }}</td>
                        <td><a href="/archivos_paciente/{{ a.id }}" target="_blank">Abrir</a></td>
                    </tr>
                    {% else %}
                    <tr><td colspan="7" style="text-align:center;">Sin archivos cargados.</td></tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <a href="/dashboard" class="btn" style="margin-top:18px;">← Volver a análisis</a>
        <a href="/" class="btn btn-secondary" style="margin-top:18px;">Menú principal</a>
    </div>
</body>
</html>
"""

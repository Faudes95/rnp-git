from __future__ import annotations

import html
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlsplit


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _path_label(segment: str) -> str:
    seg = _safe_text(segment).replace("-", " ").replace("_", " ").strip()
    if not seg:
        return "Inicio"
    specials = {
        "consulta": "Consulta",
        "consulta externa": "Consulta Externa",
        "hospitalizacion": "Hospitalización",
        "quirofano": "Quirófano",
        "reporte": "Reporte Estadístico",
        "alertas": "Alertas",
        "expediente": "Expediente Único",
        "guardia": "Guardia",
        "censo": "Censo",
        "alta": "Alta Hospitalaria",
    }
    low = seg.lower()
    if low in specials:
        return specials[low]
    return " ".join(x.capitalize() for x in seg.split())


def _build_breadcrumbs(path: str) -> List[Dict[str, str]]:
    crumbs: List[Dict[str, str]] = [{"label": "Inicio", "href": "/"}]
    clean = _safe_text(path or "/")
    if not clean.startswith("/"):
        clean = f"/{clean}"
    if clean == "/":
        return crumbs

    parts = [p for p in clean.split("/") if p]
    acc = ""
    for p in parts:
        acc += f"/{p}"
        crumbs.append({"label": _path_label(p), "href": acc})
    return crumbs


def _get_query_dict(full_url: str) -> Dict[str, str]:
    try:
        qs = parse_qs(urlsplit(full_url).query, keep_blank_values=False)
        out: Dict[str, str] = {}
        for k, vals in qs.items():
            if vals:
                out[k] = str(vals[-1])
        return out
    except Exception:
        return {}


def _extract_patient_context(request: Any, context: Dict[str, Any]) -> Dict[str, str]:
    params = _get_query_dict(str(getattr(request, "url", "") or ""))
    paciente: Dict[str, str] = {
        "consulta_id": _safe_text(params.get("consulta_id")),
        "hospitalizacion_id": _safe_text(params.get("hospitalizacion_id")),
        "nss": _safe_text(params.get("nss")),
        "nombre": _safe_text(params.get("nombre")),
    }

    consulta = context.get("consulta")
    if consulta is not None:
        if not paciente["consulta_id"]:
            paciente["consulta_id"] = _safe_text(getattr(consulta, "id", ""))
        if not paciente["nss"]:
            paciente["nss"] = _safe_text(getattr(consulta, "nss", ""))
        if not paciente["nombre"]:
            paciente["nombre"] = _safe_text(getattr(consulta, "nombre", ""))

    if not paciente["nss"]:
        paciente["nss"] = _safe_text(context.get("target_nss"))
    if not paciente["nombre"]:
        paciente["nombre"] = _safe_text(context.get("target_nombre"))

    active_hosp = context.get("active_hospitalizacion") or {}
    if isinstance(active_hosp, dict):
        if not paciente["hospitalizacion_id"]:
            paciente["hospitalizacion_id"] = _safe_text(active_hosp.get("id"))

    return paciente


def _patient_links(patient_ctx: Dict[str, str]) -> List[Dict[str, str]]:
    consulta_id = _safe_text(patient_ctx.get("consulta_id"))
    nss = _safe_text(patient_ctx.get("nss"))
    nombre = _safe_text(patient_ctx.get("nombre"))
    hosp_id = _safe_text(patient_ctx.get("hospitalizacion_id"))
    links: List[Dict[str, str]] = []

    if consulta_id:
        links.append({"label": "Expediente", "href": f"/expediente?consulta_id={consulta_id}"})
        note_href = f"/expediente/inpatient-captura?consulta_id={consulta_id}"
        if hosp_id:
            note_href += f"&hospitalizacion_id={hosp_id}"
        note_href += "#nota-diaria"
        links.append({"label": "+ Realizar nota médica", "href": note_href})
    elif nss:
        q = urlencode({"nss": nss, "nombre": nombre})
        links.append({"label": "Expediente", "href": f"/expediente?{q}"})

    if consulta_id or nss:
        params = {}
        if consulta_id:
            params["consulta_id"] = consulta_id
        if nss and not consulta_id:
            params["nss"] = nss
        if nombre and not consulta_id:
            params["nombre"] = nombre
        if hosp_id:
            params["hospitalizacion_id"] = hosp_id
        links.append({"label": "Captura estructurada", "href": f"/expediente/inpatient-captura?{urlencode(params)}#nota-diaria"})
    return links


def _suggested_actions(path: str, patient_ctx: Dict[str, str]) -> List[Dict[str, str]]:
    p = _safe_text(path).lower()
    if p.startswith("/consulta"):
        return [
            {"label": "Abrir Expediente", "href": "/expediente"},
            {"label": "Ingresar a Hospitalización", "href": "/hospitalizacion/ingresar"},
            {"label": "Ver Reporte", "href": "/reporte"},
        ]
    if p.startswith("/hospitalizacion"):
        links = [
            {"label": "Censo Diario", "href": "/hospitalizacion/censo"},
            {"label": "Guardia", "href": "/hospitalizacion/guardia"},
            {"label": "Reporte Hospitalización", "href": "/hospitalizacion/reporte"},
        ]
        links.extend(_patient_links(patient_ctx)[:1])
        return links
    if p.startswith("/quirofano"):
        return [
            {"label": "Programar Cirugía", "href": "/quirofano/programar"},
            {"label": "Urgencias", "href": "/quirofano/urgencias"},
            {"label": "Alertas", "href": "/reporte/alertas"},
        ]
    if p.startswith("/reporte"):
        return [
            {"label": "Panel General", "href": "/reporte"},
            {"label": "Alertas", "href": "/reporte/alertas"},
            {"label": "Hospitalización", "href": "/hospitalizacion/reporte"},
        ]
    if p.startswith("/expediente"):
        links = [
            {"label": "+ Realizar nota médica", "href": "/expediente/inpatient-captura#nota-diaria"},
            {"label": "Captura estructurada", "href": "/expediente/inpatient-captura"},
            {"label": "Hospitalización", "href": "/hospitalizacion"},
        ]
        if patient_ctx.get("consulta_id"):
            cid = patient_ctx["consulta_id"]
            hid = _safe_text(patient_ctx.get("hospitalizacion_id"))
            href = f"/expediente/inpatient-captura?consulta_id={cid}"
            if hid:
                href += f"&hospitalizacion_id={hid}"
            href += "#nota-diaria"
            links[0]["href"] = href
            links[1]["href"] = href
        return links
    return [
        {"label": "Consulta Externa", "href": "/consulta_externa"},
        {"label": "Hospitalización", "href": "/hospitalizacion"},
        {"label": "Quirófano", "href": "/quirofano"},
        {"label": "Reporte", "href": "/reporte"},
    ]


def inject_ui_shell(html_text: str, *, request: Any, context: Optional[Dict[str, Any]] = None) -> str:
    src = str(html_text or "")
    if not src or 'id="rnp-ui-shell"' in src:
        return src
    if request is None:
        return src

    ctx = context or {}
    path = _safe_text(getattr(getattr(request, "url", None), "path", "/") or "/")
    crumbs = _build_breadcrumbs(path)
    patient_ctx = _extract_patient_context(request, ctx)
    quick_links = [
        {"label": "Inicio", "href": "/"},
        {"label": "Consulta", "href": "/consulta_externa"},
        {"label": "Hospitalización", "href": "/hospitalizacion"},
        {"label": "Quirófano", "href": "/quirofano"},
        {"label": "Reporte", "href": "/reporte"},
        {"label": "Expediente", "href": "/expediente"},
    ]
    suggest = _suggested_actions(path, patient_ctx)
    patient_links = _patient_links(patient_ctx)

    nav_html = f"""
<section id="rnp-ui-shell" data-rnp-shell="1">
  <style>
    #rnp-ui-shell {{ font-family:'Montserrat',sans-serif; margin:0 0 12px 0; border:1px solid #dbe5df; border-radius:12px; background:linear-gradient(180deg,#f7fbf9 0%,#ffffff 100%); box-shadow:0 4px 14px rgba(0,0,0,.06); }}
    #rnp-ui-shell .row {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; padding:10px 12px; }}
    #rnp-ui-shell .top {{ border-bottom:1px solid #e5ece8; }}
    #rnp-ui-shell .brand {{ font-weight:800; color:#13322B; margin-right:8px; }}
    #rnp-ui-shell .chip {{ display:inline-flex; align-items:center; padding:5px 9px; border-radius:999px; font-size:11px; font-weight:700; border:1px solid #d6e0da; background:#fff; color:#13322B; text-decoration:none; }}
    #rnp-ui-shell .chip.gold {{ background:#B38E5D; color:#fff; border-color:#a07f52; }}
    #rnp-ui-shell .crumbs a {{ color:#13322B; text-decoration:none; font-weight:600; font-size:12px; }}
    #rnp-ui-shell .crumbs span.sep {{ opacity:.5; margin:0 4px; }}
    #rnp-ui-shell .muted {{ font-size:11px; color:#4b5563; }}
    #rnp-ui-shell .links a {{ text-decoration:none; }}
    #rnp-ui-shell .toggle {{ margin-left:auto; border:1px solid #d6e0da; background:#fff; color:#13322B; border-radius:7px; padding:5px 8px; cursor:pointer; font-weight:700; font-size:11px; }}
    #rnp-ui-shell .mini-input {{ border:1px solid #cfd8d3; border-radius:7px; padding:6px 8px; font-size:12px; min-width:120px; }}
    #rnp-ui-shell .mini-btn {{ border:1px solid #a07f52; background:#B38E5D; color:#fff; border-radius:7px; padding:6px 9px; cursor:pointer; font-weight:700; font-size:12px; }}
    #rnp-ui-shell[data-collapsed=\"1\"] .collapsible {{ display:none; }}
    @media (max-width:900px) {{ #rnp-ui-shell .brand {{ width:100%; }} }}
  </style>
  <div class="row top">
    <div class="brand">RNP Navegación Clínica</div>
    <div class="crumbs">
      {"".join(f'<a href=\"{html.escape(c.get("href","/"))}\">{html.escape(c.get("label",""))}</a><span class=\"sep\">›</span>' for c in crumbs[:-1])}
      <strong style="font-size:12px;color:#13322B;">{html.escape(crumbs[-1].get("label","Inicio"))}</strong>
    </div>
    <button class="toggle" type="button" onclick="(function(b){{var s=document.getElementById('rnp-ui-shell');if(!s)return;var n=s.getAttribute('data-collapsed')==='1'?'0':'1';s.setAttribute('data-collapsed',n);try{{localStorage.setItem('rnp_shell_collapsed',n);}}catch(e){{}}}})(this)">Mostrar/Ocultar</button>
  </div>
  <div class="row collapsible">
    <span class="muted">Accesos rápidos:</span>
    <span class="links">
      {"".join(f'<a class=\"chip\" href=\"{html.escape(x["href"])}\">{html.escape(x["label"])}</a>' for x in quick_links)}
    </span>
  </div>
  <div class="row collapsible">
    <span class="muted">Paciente en contexto:</span>
    <span class="chip" id="rnp-shell-nss">NSS: {html.escape(patient_ctx.get("nss") or "N/E")}</span>
    <span class="chip" id="rnp-shell-nombre">Nombre: {html.escape(patient_ctx.get("nombre") or "N/E")}</span>
    <span class="chip" id="rnp-shell-consulta">Consulta: {html.escape(patient_ctx.get("consulta_id") or "N/E")}</span>
    <span class="chip" id="rnp-shell-hosp">Hospitalización: {html.escape(patient_ctx.get("hospitalizacion_id") or "N/E")}</span>
    {"".join(f'<a class=\"chip gold\" href=\"{html.escape(x["href"])}\">{html.escape(x["label"])}</a>' for x in patient_links)}
  </div>
  <div class="row collapsible">
    <span class="muted">Wizard de contexto:</span>
    <input class="mini-input" id="rnp-ctx-nss" placeholder="NSS (10 dígitos)" value="{html.escape(patient_ctx.get("nss") or "")}">
    <input class="mini-input" id="rnp-ctx-nombre" placeholder="Nombre" value="{html.escape(patient_ctx.get("nombre") or "")}">
    <input class="mini-input" id="rnp-ctx-consulta" placeholder="Consulta ID" value="{html.escape(patient_ctx.get("consulta_id") or "")}">
    <input class="mini-input" id="rnp-ctx-hosp" placeholder="Hospitalización ID" value="{html.escape(patient_ctx.get("hospitalizacion_id") or "")}">
    <button class="mini-btn" type="button" id="rnp-ctx-save">Guardar contexto</button>
  </div>
  <div class="row collapsible">
    <span class="muted">Siguiente paso sugerido:</span>
    {"".join(f'<a class=\"chip\" href=\"{html.escape(x["href"])}\">{html.escape(x["label"])}</a>' for x in suggest)}
  </div>
  <script>
    (function(){{
      var shell=document.getElementById('rnp-ui-shell');
      if(!shell) return;
      try {{
        var stored=localStorage.getItem('rnp_shell_collapsed');
        if(stored==='1') shell.setAttribute('data-collapsed','1');
      }} catch(e) {{}}
      try {{
        var saveCtx=function(ctxObj){{
          try {{
            localStorage.setItem('rnp_patient_context', JSON.stringify(ctxObj||{{}}));
            document.cookie='rnp_patient_context='+encodeURIComponent(JSON.stringify(ctxObj||{{}}))+'; path=/; max-age='+(60*60*24*30)+'; SameSite=Lax';
          }} catch(e) {{}}
          try {{
            fetch('/api/v1/contexto-activo', {{
              method:'POST',
              headers: {{'Content-Type':'application/json'}},
              keepalive:true,
              body: JSON.stringify({{
                actor:'ui_shell',
                context:ctxObj||{{}},
                source_route:(window.location && window.location.pathname) ? window.location.pathname : '/'
              }})
            }}).catch(function(){{}});
          }} catch(e) {{}}
        }};
        var ctx={json.dumps(patient_ctx, ensure_ascii=False)};
        var hasAny=ctx && (ctx.nss||ctx.nombre||ctx.consulta_id||ctx.hospitalizacion_id);
        if(hasAny) saveCtx(ctx);
        if(!hasAny) {{
          var storedRaw=localStorage.getItem('rnp_patient_context');
          if(storedRaw) {{
            var stored=JSON.parse(storedRaw);
            if(stored) {{
              var eN=document.getElementById('rnp-shell-nss'); if(eN) eN.textContent='NSS: '+(stored.nss||'N/E');
              var eNm=document.getElementById('rnp-shell-nombre'); if(eNm) eNm.textContent='Nombre: '+(stored.nombre||'N/E');
              var eC=document.getElementById('rnp-shell-consulta'); if(eC) eC.textContent='Consulta: '+(stored.consulta_id||'N/E');
              var eH=document.getElementById('rnp-shell-hosp'); if(eH) eH.textContent='Hospitalización: '+(stored.hospitalizacion_id||'N/E');
              var iN=document.getElementById('rnp-ctx-nss'); if(iN && !iN.value) iN.value=(stored.nss||'');
              var iNm=document.getElementById('rnp-ctx-nombre'); if(iNm && !iNm.value) iNm.value=(stored.nombre||'');
              var iC=document.getElementById('rnp-ctx-consulta'); if(iC && !iC.value) iC.value=(stored.consulta_id||'');
              var iH=document.getElementById('rnp-ctx-hosp'); if(iH && !iH.value) iH.value=(stored.hospitalizacion_id||'');
            }}
          }}
        }}
        var saveBtn=document.getElementById('rnp-ctx-save');
        if(saveBtn){{
          saveBtn.addEventListener('click', function(){{
            var next={{
              nss:(document.getElementById('rnp-ctx-nss')||{{}}).value||'',
              nombre:(document.getElementById('rnp-ctx-nombre')||{{}}).value||'',
              consulta_id:(document.getElementById('rnp-ctx-consulta')||{{}}).value||'',
              hospitalizacion_id:(document.getElementById('rnp-ctx-hosp')||{{}}).value||'',
              source:'wizard'
            }};
            var eN=document.getElementById('rnp-shell-nss'); if(eN) eN.textContent='NSS: '+(next.nss||'N/E');
            var eNm=document.getElementById('rnp-shell-nombre'); if(eNm) eNm.textContent='Nombre: '+(next.nombre||'N/E');
            var eC=document.getElementById('rnp-shell-consulta'); if(eC) eC.textContent='Consulta: '+(next.consulta_id||'N/E');
            var eH=document.getElementById('rnp-shell-hosp'); if(eH) eH.textContent='Hospitalización: '+(next.hospitalizacion_id||'N/E');
            saveCtx(next);
          }});
        }}
      }} catch(e) {{}}
      try {{
        var path=window.location.pathname||'/';
        var key='rnp_ui_nav_sent:'+path;
        var uiErrKey='rnp_ui_err_hooked';
        var sendUiError=function(payload){{
          try {{
            var body=Object.assign({{
              path:path,
              actor:'ui_shell',
              severity:'ERROR'
            }}, payload||{{}});
            fetch('/api/v1/ui/error-event', {{
              method:'POST',
              headers: {{'Content-Type':'application/json'}},
              keepalive:true,
              body: JSON.stringify(body)
            }}).catch(function(){{}});
          }} catch(e) {{}}
        }};
        if(!window[uiErrKey]) {{
          window[uiErrKey]=true;
          window.addEventListener('error', function(ev){{
            try {{
              var target=ev && ev.target ? ev.target : null;
              var isResource=target && (target.tagName==='IMG' || target.tagName==='SCRIPT' || target.tagName==='LINK');
              if(isResource) {{
                sendUiError({{
                  event_type:'RESOURCE_ERROR',
                  message:'Error cargando recurso UI',
                  source:(target.tagName||'')+':'+(target.src||target.href||''),
                  context:{{ tag: target.tagName||'', src: target.src||target.href||'' }}
                }});
                return;
              }}
              sendUiError({{
                event_type:'JS_ERROR',
                message:String((ev && ev.message) || 'JS error'),
                source:String((ev && ev.filename) || ''),
                stack:String((ev && ev.error && ev.error.stack) || '')
              }});
            }} catch(inner) {{}}
          }}, true);
          window.addEventListener('unhandledrejection', function(ev){{
            try {{
              var reason=ev ? ev.reason : null;
              var msg=(reason && reason.message) ? reason.message : String(reason || 'Unhandled rejection');
              var stk=(reason && reason.stack) ? reason.stack : '';
              sendUiError({{
                event_type:'UNHANDLED_REJECTION',
                message:msg,
                stack:String(stk||'')
              }});
            }} catch(inner) {{}}
          }});
        }}
        if(!sessionStorage.getItem(key)) {{
          sessionStorage.setItem(key,'1');
          var ctxRaw=localStorage.getItem('rnp_patient_context')||'{{}}';
          var ctxObj={{}};
          try {{ ctxObj=JSON.parse(ctxRaw)||{{}}; }} catch(err) {{ ctxObj={{}}; }}
          fetch('/api/v1/ui/nav-event', {{
            method:'POST',
            headers: {{'Content-Type':'application/json'}},
            keepalive:true,
            body: JSON.stringify({{
              event_type:'PAGE_VIEW',
              path:path,
              referrer:document.referrer||'',
              stage:'page_load',
              context:{{
                consulta_id: ctxObj.consulta_id || '',
                hospitalizacion_id: ctxObj.hospitalizacion_id || '',
                has_nss: !!ctxObj.nss
              }}
            }})
          }}).catch(function(err){{
            sendUiError({{
              event_type:'FETCH_ERROR',
              message:'Error enviando evento de navegación',
              source:'/api/v1/ui/nav-event',
              stack:String((err && err.message) || err || '')
            }});
          }});
        }}
      }} catch(e) {{}}
    }})();
  </script>
</section>
"""

    body_re = re.compile(r"(<body[^>]*>)", flags=re.IGNORECASE)
    m = body_re.search(src)
    if m:
        insert_pos = m.end()
        return src[:insert_pos] + nav_html + src[insert_pos:]
    return nav_html + src

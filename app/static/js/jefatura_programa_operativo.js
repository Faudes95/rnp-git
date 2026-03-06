// =================== DATA ===================
let SCHEDULE_ROWS = [{"Residente": "Avila Contreras O.", "Gdo": "R5", "MAR": "HGR72", "ABR": "HGZ48", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "HEMCNR", "AGO": "HEMCNR", "SEP": "ANDRO", "OCT": "PEDIA", "NOV": "HEMCNR", "DIC": "HEMCNR", "ENE": "HGZ29", "FEB": "HEMCNR"}, {"Residente": "Mauleon Palacios J.", "Gdo": "R5", "MAR": "HGZ48", "ABR": "PEDIA", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "HGZ29", "AGO": "HEMCNR", "SEP": "HGR72", "OCT": "HEMCNR", "NOV": "ANDRO", "DIC": "HEMCNR", "ENE": "HEMCNR", "FEB": "HEMCNR"}, {"Residente": "Navarro Tomas M.", "Gdo": "R5", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "HGR72", "JUN": "ANDRO", "JUL": "HEMCNR", "AGO": "HEMCNR", "SEP": "PEDIA", "OCT": "HGZ29", "NOV": "HEMCNR", "DIC": "HEMCNR", "ENE": "HEMCNR", "FEB": "HGZ48"}, {"Residente": "Perez Becerra J.", "Gdo": "R5", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "HGZ29", "JUN": "PEDIA", "JUL": "HGZ48", "AGO": "HEMCNR", "SEP": "HEMCNR", "OCT": "HGR72", "NOV": "HEMCNR", "DIC": "ANDRO", "ENE": "HEMCNR", "FEB": "HEMCNR"}, {"Residente": "Rodriguez Lira J.", "Gdo": "R5", "MAR": "HGZ29", "ABR": "HEMCNR", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "HGR72", "AGO": "ANDRO", "SEP": "HEMCNR", "OCT": "HEMCNR", "NOV": "HGZ48", "DIC": "PEDIA", "ENE": "HEMCNR", "FEB": "HEMCNR"}, {"Residente": "Saavedra Vazquez I.", "Gdo": "R5", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "ANDRO", "JUN": "HGZ48", "JUL": "HEMCNR", "AGO": "HGR72", "SEP": "HEMCNR", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "HEMCNR", "ENE": "PEDIA", "FEB": "HGZ29"}, {"Residente": "Torres Martinez L.", "Gdo": "R5", "MAR": "HEMCNR", "ABR": "HGR72", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "HEMCNR", "AGO": "HEMCNR", "SEP": "PEDIA", "OCT": "HEMCNR", "NOV": "HGZ29", "DIC": "HGZ48", "ENE": "ANDRO", "FEB": "HEMCNR"}, {"Residente": "Alvarado Baños F.", "Gdo": "R4", "MAR": "ONCO [V: 24-31]", "ABR": "ONCO [V: 01-08]", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "HEMCNR", "AGO": "HGZ25", "SEP": "HGZ27", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "HEMCNR", "ENE": "HEMCNR", "FEB": "HGZ24 [V: 09-22]"}, {"Residente": "Camacho Carbajal R.", "Gdo": "R4", "MAR": "HEMCNR", "ABR": "HGR25", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "HGZ24 [V: 03-16]", "AGO": "HEMCNR", "SEP": "HEMCNR", "OCT": "ONCO [V: 29-31]", "NOV": "ONCO [V: 01-11]", "DIC": "HGZ57", "ENE": "HEMCNR", "FEB": "HEMCNR"}, {"Residente": "Casas Aguilar G.", "Gdo": "R4", "MAR": "HEMCNR", "ABR": "HGZ57", "MAY": "HGZ24 [V: 08-21]", "JUN": "HEMCNR", "JUL": "HEMCNR", "AGO": "HEMCNR", "SEP": "HGR25", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "ONCO", "ENE": "ONCO [V: 26-31]", "FEB": "ONCO [V: 01-08]"}, {"Residente": "Gonzalez Salas J.", "Gdo": "R4", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "HEMCNR", "JUN": "HGZ24 [V: 05-18]", "JUL": "HGR25", "AGO": "HEMCNR", "SEP": "HEMCNR", "OCT": "HGZ57", "NOV": "ONCO [V: 27-30]", "DIC": "ONCO [V: 01-10]", "ENE": "HEMCNR", "FEB": "HEMCNR"}, {"Residente": "Granados Rivera D.", "Gdo": "R4", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "HGZ25", "JUN": "HGR57", "JUL": "HEMCNR", "AGO": "ONCO [V: 14-27]", "SEP": "ONCO", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "HGZ24", "ENE": "HGZ25 [V: 26-31]", "FEB": "HEMCNR [V: 01-08]"}, {"Residente": "Martinez Guzman G.", "Gdo": "R4", "MAR": "HGZ57", "ABR": "HEMCNR", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "ONCO [V: 31]", "AGO": "ONCO [V: 01-13]", "SEP": "HEMCNR", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "HGZ24", "ENE": "HGZ25 [V: 12-25]", "FEB": "HEMCNR"}, {"Residente": "Ortiz Avalos M.", "Gdo": "R4", "MAR": "HGZ24", "ABR": "HEMCNR", "MAY": "ONCO [V: 08-21]", "JUN": "ONCO", "JUL": "HEMCNR", "AGO": "HEMCNR", "SEP": "HEMCNR", "OCT": "HGZ25", "NOV": "HGZ57 [V: 12-26]", "DIC": "HEMCNR", "ENE": "HEMCNR", "FEB": "HEMCNR"}, {"Residente": "Diaz Martinez E.", "Gdo": "R3", "MAR": "HGR72 [V: 09-23]", "ABR": "HEMCNR", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "HEMCNR", "AGO": "HEMCNR", "SEP": "HEMCNR", "OCT": "HEMCNR", "NOV": "HGZ27 [V: 29-30]", "DIC": "ONCO [V: 01-11]", "ENE": "ONCO", "FEB": "HGZ29"}, {"Residente": "Corral Moreno R.", "Gdo": "R3", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "HEMCNR", "JUN": "HEMCNR", "JUL": "ONCO [V: 31]", "AGO": "ONCO [V: 01-13]", "SEP": "HGZ27", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "HGR72 [V: 12-26]", "ENE": "HEMCNR", "FEB": "HGZ27"}, {"Residente": "Garcia Padilla R.", "Gdo": "R3", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "HEMCNR", "JUN": "ONCO [V: 19-30]", "JUL": "ONCO [V: 01-02]", "AGO": "HGR72", "SEP": "HEMCNR", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "HGZ29", "ENE": "HEMCNR", "FEB": "HEMCNR [V: 09-22]"}, {"Residente": "Gonzalez Jimenez Y.", "Gdo": "R3", "MAR": "HEMCNR", "ABR": "HGZ27 [V: 09-22]", "MAY": "HEMCNR", "JUN": "HGR29", "JUL": "HEMCNR", "AGO": "HEMCNR", "SEP": "HEMCNR", "OCT": "HGZ72", "NOV": "ONCO [V: 27-30]", "DIC": "ONCO [V: 01-10]", "ENE": "HGZ29", "FEB": "HEMCNR"}, {"Residente": "Hernandez Gutierrez", "Gdo": "R3", "MAR": "ONCO [V: 24-31]", "ABR": "ONCO [V: 01-08]", "MAY": "HEMCNR", "JUN": "HGR72", "JUL": "HEMCNR", "AGO": "HGZ27", "SEP": "HEMCNR", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "HEMCNR", "ENE": "HEMCNR [V: 12-25]", "FEB": "HGR72"}, {"Residente": "Jauregui Diaz J.", "Gdo": "R3", "MAR": "HGZ27", "ABR": "HEMCNR", "MAY": "HGZ29 [V: 08-21]", "JUN": "HEMCNR", "JUL": "HEMCNR", "AGO": "HEMCNR", "SEP": "HGR72 [V: 17-30]", "OCT": "HGZ27", "NOV": "HEMCNR", "DIC": "HEMCNR", "ENE": "HEMCNR", "FEB": "HEMCNR"}, {"Residente": "Rodriguez Alvarado", "Gdo": "R3", "MAR": "HEMCNR", "ABR": "ONCO [V: 23-30]", "MAY": "ONCO [V: 01-07]", "JUN": "HEMCNR", "JUL": "HEMCNR", "AGO": "HGZ29", "SEP": "HGR72", "OCT": "HGZ27 [V: 15-28]", "NOV": "HEMCNR", "DIC": "HEMCNR", "ENE": "ONCO", "FEB": "ONCO"}, {"Residente": "Samperio Gomez S.", "Gdo": "R3", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "HGR72", "JUN": "HEMCNR", "JUL": "HGZ27 [V: 17-30]", "AGO": "HEMCNR", "SEP": "HGZ29", "OCT": "HEMCNR", "NOV": "HEMCNR", "DIC": "HEMCNR", "ENE": "HGZ27 [V: 26-31]", "FEB": "HEMCNR [V: 01-08]"}, {"Residente": "Torres Ramirez J.", "Gdo": "R3", "MAR": "HEMCNR", "ABR": "HEMCNR", "MAY": "HEMCNR", "JUN": "HGZ29 [V: 05-18]", "JUL": "HGR72", "AGO": "HEMCNR", "SEP": "HEMCNR", "OCT": "ONCO [V: 12-25]", "NOV": "ONCO", "DIC": "HEMCNR", "ENE": "HGZ27 [V: 12-25]", "FEB": "HEMCNR"}, {"Residente": "Benitez Alday P.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}, {"Residente": "Benitez Flores F.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}, {"Residente": "Campoverde A. F.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}, {"Residente": "Palos Roberto D.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}, {"Residente": "Ríos García C.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}, {"Residente": "Samia Ramírez F.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}, {"Residente": "Serrano Alvarado J.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}, {"Residente": "Tapia González R.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}, {"Residente": "Vital Lara C.", "Gdo": "R2", "MAR": "PENDIENTE", "ABR": "PENDIENTE", "MAY": "PENDIENTE", "JUN": "PENDIENTE", "JUL": "PENDIENTE", "AGO": "PENDIENTE", "SEP": "PENDIENTE", "OCT": "PENDIENTE", "NOV": "PENDIENTE", "DIC": "PENDIENTE", "ENE": "PENDIENTE", "FEB": "PENDIENTE"}];
let LONG_DATA = [];

function parseVacFromAsignacion(asignacion){
  const m = String(asignacion || "").match(/\[\s*V\s*:\s*(\d{1,2})\s*-\s*(\d{1,2})\s*\]/i);
  if(!m) return { ini: null, fin: null, dias: 0 };
  const ini = parseInt(m[1], 10);
  const fin = parseInt(m[2], 10);
  if(!Number.isFinite(ini) || !Number.isFinite(fin)) return { ini: null, fin: null, dias: 0 };
  const dias = Math.max(0, fin - ini + 1);
  return { ini, fin, dias };
}

function buildLongData(rows){
  const out = [];
  for(const row of rows){
    for(const mes of MONTHS){
      const asignacion = String(row[mes] || "").trim();
      const rotacion = asignacion.replace(/\s*\[V:.*?\]\s*/gi, "").trim();
      const vac = parseVacFromAsignacion(asignacion);
      out.push({
        Residente: row.Residente,
        Gdo: row.Gdo,
        Mes: mes,
        Asignacion: asignacion,
        Rotacion: rotacion,
        Vac_ini: vac.ini,
        Vac_fin: vac.fin,
        Vac_dias: vac.dias,
        Es_pendiente: /\bPENDIENTE\b/i.test(asignacion)
      });
    }
  }
  return out;
}

function rebuildLongData(){
  LONG_DATA = buildLongData(SCHEDULE_ROWS);
}

function rebuildVacSegments(){
  VAC_SEGMENTS = buildVacSegmentsFromSchedule(SCHEDULE_ROWS);
}

function rebuildDerivedData(){
  rebuildLongData();
  rebuildVacSegments();
}


const BASE_SCHEDULE_ROWS = JSON.parse(JSON.stringify(SCHEDULE_ROWS));
(function loadScheduleOverride(){
  try{
    const saved = JSON.parse(localStorage.getItem("sched_rows_v1")||"null");
    if(Array.isArray(saved) && saved.length){
      if(saved[0] && typeof saved[0]==="object" && ("Residente" in saved[0]) && ("Gdo" in saved[0])){
        SCHEDULE_ROWS = saved;
      }
    }
  } catch(e){}
})();


let VAC_SEGMENTS = [];

function buildVacSegmentsFromSchedule(rows){
  const segments = [];
  for(const row of rows){
    for(const mes of MONTHS){
      const asignacion = String(row[mes] || "").trim();
      const rotacion = asignacion.replace(/\s*\[V:.*?\]\s*/gi, "").trim();
      const vac = parseVacFromAsignacion(asignacion);
      if(!Number.isFinite(vac.ini) || !Number.isFinite(vac.fin)) continue;
      segments.push({
        Residente: row.Residente,
        Gdo: row.Gdo,
        Mes: mes,
        Ini: vac.ini,
        Fin: vac.fin,
        Rotacion: rotacion
      });
    }
  }
  return segments;
}

// =================== CONSTANTS ===================
const MONTHS = ["MAR","ABR","MAY","JUN","JUL","AGO","SEP","OCT","NOV","DIC","ENE","FEB"];
const MONTH_TO_NUM = {MAR:3, ABR:4, MAY:5, JUN:6, JUL:7, AGO:8, SEP:9, OCT:10, NOV:11, DIC:12, ENE:1, FEB:2};
const NUM_TO_MONTH = {1:"ENE",2:"FEB",3:"MAR",4:"ABR",5:"MAY",6:"JUN",7:"JUL",8:"AGO",9:"SEP",10:"OCT",11:"NOV",12:"DIC"};

rebuildDerivedData();

function norm(s){
  return (s||"")
    .toLowerCase()
    .normalize("NFD").replace(/\p{Diacritic}/gu,"")
    .replace(/[^a-z0-9\s\.]/g," ")
    .replace(/\s+/g," ").trim();
}
function toast(msg){
  const el = document.getElementById('toast');
  if(!el){ console.log(msg); return; }
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(window.__toastTimer);
  window.__toastTimer = setTimeout(()=>el.classList.remove('show'), 2200);
}
function toISO(d){ return d.toISOString().slice(0,10); }
function parseISO(s){ const [y,m,d]=s.split("-").map(x=>parseInt(x,10)); return new Date(y, m-1, d); }
function fmtDateES(d){
  const months=["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];
  return String(d.getDate()).padStart(2,"0")+" "+months[d.getMonth()]+" "+d.getFullYear();
}
function daysInclusive(a,b){
  const ms=24*60*60*1000;
  return Math.floor((b-a)/ms)+1;
}
function overlapDays(aStart, aEnd, bStart, bEnd){
  const s = (aStart>bStart) ? aStart : bStart;
  const e = (aEnd<bEnd) ? aEnd : bEnd;
  if(e < s) return 0;
  return daysInclusive(s,e);
}
function cycleYearForMonthCode(mes, startYear){
  return (["ENE","FEB"].includes(mes)) ? (startYear+1) : startYear;
}
function monthCodeFromDate(d){ return NUM_TO_MONTH[d.getMonth()+1]; }
function nextMonthCode(mes){
  const i=MONTHS.indexOf(mes);
  if(i<0 || i===MONTHS.length-1) return null;
  return MONTHS[i+1];
}
function getCell(residente, mes){
  const row = SCHEDULE_ROWS.find(r => r.Residente===residente);
  if(!row) return "";
  return row[mes] || "";
}
function getRotation(residente, mes){
  const row = SCHEDULE_ROWS.find(r => r.Residente===residente);
  if(!row) return "";
  const cell = (row[mes]||"").trim();
  return cell.replace(/\s*\[V:.*?\]\s*/g,"").trim();
}
function getFilteredResidents(){
  const gdo = document.getElementById("gdoSel").value;
  const q = norm(document.getElementById("resSearch").value);
  let list = SCHEDULE_ROWS.slice();
  if(gdo!=="ALL") list = list.filter(r => r.Gdo===gdo);
  if(q) list = list.filter(r => norm(r.Residente).includes(q));
  list.sort((a,b)=> (a.Gdo.localeCompare(b.Gdo) || a.Residente.localeCompare(b.Residente)));
  return list;
}


function cssVar(name){
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}
function getTheme(){
  return {
    text: cssVar('--text'),
    muted: cssVar('--muted'),
    grid: cssVar('--grid'),
    blue: cssVar('--blue'),
    blueSoft: cssVar('--blue-soft'),
    red: cssVar('--red'),
    redSoft: cssVar('--red-soft')
  };
}

// ------------------- Vacations (from segments + manual) -------------------
function loadManualVac(){
  try {
    const raw = localStorage.getItem("vac_manual");
    if(!raw) return [];
    const arr = JSON.parse(raw);
    if(!Array.isArray(arr)) return [];
    return arr.filter(x=>x && x.residente && x.start && x.end);
  } catch(e) { return []; }
}
function saveManualVac(arr){ localStorage.setItem("vac_manual", JSON.stringify(arr)); }

function buildVacationPeriods(startYear){
  const byRes = new Map();
  for(const seg of VAC_SEGMENTS){
    const y = cycleYearForMonthCode(seg.Mes, startYear);
    const mnum = MONTH_TO_NUM[seg.Mes];
    const s = new Date(y, mnum-1, seg.Ini);
    const e = new Date(y, mnum-1, seg.Fin);
    if(!byRes.has(seg.Residente)) byRes.set(seg.Residente, []);
    byRes.get(seg.Residente).push({source:"matriz", start:s, end:e, gdo: seg.Gdo});
  }
  const manual = loadManualVac();
  for(const p of manual){
    const r = p.residente;
    if(!byRes.has(r)) byRes.set(r, []);
    byRes.get(r).push({source:"manual", start: parseISO(p.start), end: parseISO(p.end), gdo: (SCHEDULE_ROWS.find(x=>x.Residente===r)?.Gdo)||""});
  }

  const periods=[];
  for(const [res, segs] of byRes.entries()){
    segs.sort((a,b)=>a.start-b.start);
    let cur=null;
    for(const s of segs){
      if(!cur){ cur={...s}; continue; }
      const oneDay = 24*60*60*1000;
      if(s.start <= new Date(cur.end.getTime()+oneDay)){
        if(s.end>cur.end) cur.end=s.end;
        if(cur.source!==s.source) cur.source="mixto";
      } else {
        periods.push({residente: res, gdo: cur.gdo || (SCHEDULE_ROWS.find(x=>x.Residente===res)?.Gdo)||"", start: cur.start, end: cur.end, source: cur.source});
        cur={...s};
      }
    }
    if(cur) periods.push({residente: res, gdo: cur.gdo || (SCHEDULE_ROWS.find(x=>x.Residente===res)?.Gdo)||"", start: cur.start, end: cur.end, source: cur.source});
  }

  periods.sort((a,b)=> (a.residente.localeCompare(b.residente) || a.start-b.start));
  const idxMap=new Map();
  for(const p of periods){
    idxMap.set(p.residente, (idxMap.get(p.residente)||0)+1);
    p.periodo = idxMap.get(p.residente);
    p.dias = daysInclusive(p.start, p.end);
  }
  return periods;
}
function isOnVacation(periods, residente, d){
  for(const p of periods){
    if(p.residente===residente && d>=p.start && d<=p.end) return p;
  }
  return null;
}
function nextVacation(periods, residente, d){
  const future = periods.filter(p=>p.residente===residente && p.start>d).sort((a,b)=>a.start-b.start);
  return future[0] || null;
}

// ------------------- UI INIT -------------------
const gdoSel = document.getElementById("gdoSel");
const mesSel = document.getElementById("mesSel");
const refDate = document.getElementById("refDate");
const startYear = document.getElementById("startYear");

function initControls(){
  const now = new Date();
  refDate.value = toISO(now);
  const sy = (now.getMonth()+1>=3) ? now.getFullYear() : (now.getFullYear()-1);
  startYear.value = sy;

  const grados = ["ALL", ...Array.from(new Set(SCHEDULE_ROWS.map(r=>r.Gdo))).sort()];
  gdoSel.innerHTML = grados.map(g=>`<option value="${g}">${g==="ALL"?"Todos":g}</option>`).join("");
  mesSel.innerHTML = ["ALL", ...MONTHS].map(m=>`<option value="${m}">${m==="ALL"?"Todos":m}</option>`).join("");
  const covMesEl = document.getElementById("covMes");
  if(covMesEl){ covMesEl.innerHTML = MONTHS.map(m=>`<option value="${m}">${m}</option>`).join(""); }
}


// ------------------- CSV helpers -------------------
function makeCSV(objs){
  if(!objs.length) return "";
  const cols = Object.keys(objs[0]);
  const esc = (v)=> {
    const s = (v===null||v===undefined) ? "" : String(v);
    if(/[",\n]/.test(s)) return '"' + s.replace(/"/g,'""') + '"';
    return s;
  };
  const lines = [cols.join(",")];
  for(const o of objs) lines.push(cols.map(c=>esc(o[c])).join(","));
  return lines.join("\n");
}
function download(name, content){
  const blob = new Blob([content], {type:"text/csv;charset=utf-8"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = name; a.click();
  URL.revokeObjectURL(url);
}

// ------------------- KPIs -------------------
function updateKPIs(periods){
  const resList = getFilteredResidents();
  const mes = mesSel.value;
  const sy = parseInt(startYear.value,10);
  const dRef = parseISO(refDate.value);
  const mesEff = (mes!=="ALL") ? mes : monthCodeFromDate(dRef);
  const yMes = cycleYearForMonthCode(mesEff, sy);
  const mNum = MONTH_TO_NUM[mesEff];
  const d = (mes!=="ALL") ? new Date(yMes, mNum-1, 15) : dRef;

  const filteredLong = LONG_DATA.filter(r => {
    const okG = (gdoSel.value==="ALL" || r.Gdo===gdoSel.value);
    const okM = (mes==="ALL" || r.Mes===mes);
    const okR = (!document.getElementById("resSearch").value) || norm(r.Residente).includes(norm(document.getElementById("resSearch").value));
    return okG && okM && okR;
  });

  const pend = filteredLong.filter(x=>x.Es_pendiente).length;
  const uniqRot = new Set(filteredLong.map(x=>x.Rotacion).filter(Boolean)).size;
  const vacDays = filteredLong.reduce((acc,x)=>acc+(x.Vac_dias||0),0);

  let vacNow=0;
  for(const r of resList) if(isOnVacation(periods, r.Residente, d)) vacNow++;

  document.getElementById("kRes").textContent = resList.length;
  document.getElementById("kPend").textContent = pend;
  document.getElementById("kRot").textContent = uniqRot;
  document.getElementById("kVacDays").textContent = vacDays;
  document.getElementById("kVacNow").textContent = vacNow;
}

// ------------------- Charts -------------------
let rotChart=null;
let vacChart=null;

function updateRotChart(){
  const mes = mesSel.value==="ALL" ? monthCodeFromDate(parseISO(refDate.value)) : mesSel.value;
  const resList = getFilteredResidents().map(r=>r.Residente);
  const rows = LONG_DATA.filter(x => x.Mes===mes && resList.includes(x.Residente));
  const counts = new Map();
  for(const r of rows){
    const k = r.Rotacion || "(sin dato)";
    counts.set(k, (counts.get(k)||0)+1);
  }
  const entries = Array.from(counts.entries()).sort((a,b)=>b[1]-a[1]);
  const labels = entries.map(e=>e[0]);
  const data = entries.map(e=>e[1]);

  const ctx = document.getElementById("rotChart").getContext("2d");
  if(rotChart) rotChart.destroy();
  rotChart = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets: [{ label: "Residentes", data, backgroundColor: getTheme().blueSoft, borderColor: getTheme().blue, borderWidth: 1.5, borderRadius: 10 }] },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (c)=> ` ${c.parsed.y} residentes` } }
      },
      scales: {
        x: { ticks: { color: getTheme().muted }, grid: { color: getTheme().grid } },
        y: { ticks: { color: getTheme().muted }, grid: { color: getTheme().grid }, beginAtZero: true, precision: 0 }
      }
    }
  });

  const top = entries.slice(0,8).map(([k,v]) => `
    <div style="display:flex; justify-content:space-between; gap:10px; padding:8px 0; border-bottom:1px solid var(--line);">
      <div><span class="badge b-info">${k}</span></div><div style="font-weight:700">${v}</div>
    </div>`).join("");
  document.getElementById("topRot").innerHTML = top || '<div class="note">Sin datos para este filtro.</div>';
}

function updateVacChart(periods){
  const resList = getFilteredResidents().map(r=>r.Residente);
  const filtered = periods.filter(p => resList.includes(p.residente) && (gdoSel.value==="ALL" || p.gdo===gdoSel.value));

  const mes = mesSel.value;
  const sy = parseInt(startYear.value,10);

  let labels = MONTHS.slice();
  let data = [];

  if(mes!=="ALL"){
    const y = cycleYearForMonthCode(mes, sy);
    const mnum = MONTH_TO_NUM[mes];
    const mStart = new Date(y, mnum-1, 1);
    const mEnd = new Date(y, mnum, 0);
    const sum = filtered.reduce((acc,p)=> acc + overlapDays(p.start, p.end, mStart, mEnd), 0);
    labels = [mes];
    data = [sum];
    document.getElementById("vacHint").innerHTML = `Total (filtro, ${mes}): <b>${sum}</b> día(s) de vacaciones en el mes.`;
  } else {
    const sums = new Map(MONTHS.map(m=>[m,0]));
    for(const p of filtered){
      let cur = new Date(p.start);
      while(cur<=p.end){
        const mcode = NUM_TO_MONTH[cur.getMonth()+1];
        if(sums.has(mcode)) sums.set(mcode, sums.get(mcode)+1);
        cur = new Date(cur.getTime()+24*60*60*1000);
      }
    }
    labels = MONTHS.slice();
    data = labels.map(m=>sums.get(m)||0);
    const total = filtered.reduce((a,p)=>a+p.dias,0);
    document.getElementById("vacHint").innerHTML = `Total (filtro): <b>${total}</b> día(s) de vacaciones consolidados.`;
  }

  const ctx = document.getElementById("vacChart").getContext("2d");
  if(vacChart) vacChart.destroy();
  vacChart = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets: [{ label: "Días de vacaciones", data, backgroundColor: getTheme().redSoft, borderColor: getTheme().red, borderWidth: 1.5, borderRadius: 10 }] },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (c)=> ` ${c.parsed.y} día(s)` } }
      },
      scales: {
        x: { ticks: { color: getTheme().muted }, grid: { color: getTheme().grid } },
        y: { ticks: { color: getTheme().muted }, grid: { color: getTheme().grid }, beginAtZero: true, precision: 0 }
      }
    }
  });
}


// ------------------- Tables -------------------
function renderMatrix(){
  const resList = getFilteredResidents();
  const tbl = document.getElementById("matrixTbl");
  const head = `<tr><th style="min-width:220px;">Residente</th><th>Gdo</th>${MONTHS.map(m=>`<th>${m}</th>`).join("")}</tr>`;
  const rows = resList.map(r=>{
    const cells = MONTHS.map(m=>{
      const v = getCell(r.Residente, m);
      const isPend = /\bPENDIENTE\b/i.test(v);
      const hasV = /\[V:/i.test(v);
      const badge = isPend ? '<span class="badge b-bad">PENDIENTE</span>' : (hasV ? '<span class="badge b-warn">V</span>' : '<span class="badge b-ok">OK</span>');
      return `<td><div style="display:flex; gap:6px; align-items:center; flex-wrap:wrap;"><span>${v||""}</span>${badge}</div></td>`;
    }).join("");
    return `<tr><td>${r.Residente}</td><td><span class="badge b-info">${r.Gdo}</span></td>${cells}</tr>`;
  }).join("");
  tbl.innerHTML = head + rows;
}

function renderStatus(periods){
  const resList = getFilteredResidents();

  const sy = parseInt(startYear.value,10);
  const dRef = parseISO(refDate.value);
  const mesSelVal = mesSel.value;

  // Si el usuario selecciona un mes específico, el "estado operativo" se calcula para ese mes
  // usando una fecha representativa (día 15) del mes dentro del ciclo MAR→FEB.
  const mes = (mesSelVal!=="ALL") ? mesSelVal : monthCodeFromDate(dRef);
  const yMes = cycleYearForMonthCode(mes, sy);
  const mNum = MONTH_TO_NUM[mes];
  const d = (mesSelVal!=="ALL") ? new Date(yMes, mNum-1, 15) : dRef;

  const monthStart = new Date(yMes, mNum-1, 1);
  const monthEnd = new Date(yMes, mNum, 0); // último día del mes

  const nextMes = nextMonthCode(mes);

  const tbl = document.getElementById("statusTbl");
  const head = `<tr>
    <th style="min-width:220px;">Residente</th>
    <th>Gdo</th>
    <th>Mes</th>
    <th>Rotación programada</th>
    <th>Ubicación actual</th>
    <th>Próxima rotación</th>
    <th>Estatus vacacional</th>
    <th>Inicio</th>
    <th>Fin</th>
  </tr>`;

  const rows = resList.map(r=>{
    const rot = getRotation(r.Residente, mes) || "(sin dato)";
    const rotNext = nextMes ? (getRotation(r.Residente, nextMes) || "(sin dato)") : "—";

    // Modo "MES": mostrar si tiene vacaciones EN ESE MES (aunque no caigan justo en el día 15)
    const monthMode = (mesSelVal!=="ALL");
    let statusBadge = '<span class="badge b-ok">Activo</span>';
    let ubic = rot;
    let ubicExtra = "";
    let ini=""; let fin="";

    if(monthMode){
      const overlaps = periods
        .filter(p => p.residente===r.Residente && overlapDays(p.start, p.end, monthStart, monthEnd)>0)
        .sort((a,b)=>a.start-b.start);

      if(overlaps.length){
        const segs = overlaps.map(p=>{
          const s = (p.start < monthStart) ? monthStart : p.start;
          const e = (p.end > monthEnd) ? monthEnd : p.end;
          return {s,e};
        });

        const label = segs.map(x=>{
          const ds = String(x.s.getDate()).padStart(2,"0");
          const de = String(x.e.getDate()).padStart(2,"0");
          return `${ds}-${de}`;
        }).join(", ");

        statusBadge = `<span class="badge b-warn">Vacaciones: ${label}</span>`;
        ubicExtra = `<div class="cell-sub"><span class="tag-vac">VACACIONES</span><span class="tag-vac-range">${label}</span></div>`;
        ini = segs.map(x=>fmtDateES(x.s)).join("<br>");
        fin = segs.map(x=>fmtDateES(x.e)).join("<br>");
      } else {
        // también puedes ver próximas con base al día 15, por utilidad operativa
        const pNext = nextVacation(periods, r.Residente, d);
        if(pNext){
          statusBadge = `<span class="badge b-info">Próximas: ${fmtDateES(pNext.start)}</span>`;
        }
      }
    } else {
      // Modo "FECHA": comportamiento original
      const pNow = isOnVacation(periods, r.Residente, d);
      const pNext = nextVacation(periods, r.Residente, d);

      if(pNow){
        statusBadge = '<span class="badge b-warn">En vacaciones</span>';
        ubicExtra = `<div class="cell-sub"><span class="tag-vac">VACACIONES</span><span class="tag-vac-range">${fmtDateES(pNow.start)} – ${fmtDateES(pNow.end)}</span></div>`;
        ini = fmtDateES(pNow.start);
        fin = fmtDateES(pNow.end);
      } else if(pNext){
        statusBadge = `<span class="badge b-info">Próximas: ${fmtDateES(pNext.start)}</span>`;
      }
    }

    return `<tr>
      <td>${r.Residente}</td>
      <td><span class="badge b-info">${r.Gdo}</span></td>
      <td><span class="badge">${mes}</span></td>
      <td>${rot}</td>
      <td><div class="cell-stack"><b>${ubic}</b>${ubicExtra||""}</div></td>
      <td>${rotNext}</td>
      <td>${statusBadge}</td>
      <td>${ini}</td>
      <td>${fin}</td>
    </tr>`;
  }).join("");

  tbl.innerHTML = head + rows;
}


function renderVacations(periods){
  const resList = getFilteredResidents().map(r=>r.Residente);
  const gdo = gdoSel.value;
  const q = norm(document.getElementById("resSearch").value);
  const mes = mesSel.value;
  const sy = parseInt(startYear.value,10);

  let monthStart=null, monthEnd=null;
  if(mes!=="ALL"){
    const y = cycleYearForMonthCode(mes, sy);
    const mnum = MONTH_TO_NUM[mes];
    monthStart = new Date(y, mnum-1, 1);
    monthEnd = new Date(y, mnum, 0);
  }

  let filtered = periods.filter(p => resList.includes(p.residente));
  if(gdo!=="ALL") filtered = filtered.filter(p=>p.gdo===gdo);
  if(q) filtered = filtered.filter(p=>norm(p.residente).includes(q));
  if(monthStart){
    // Mostrar solo periodos que tocan el mes seleccionado (incluye soporte multi‑mes)
    filtered = filtered.filter(p => p.start <= monthEnd && p.end >= monthStart);
  }

  // Días aplicables al mes (para que "Grado + Mes + Vacaciones" sea exacto)
  for(const p of filtered){
    p.dias_en_mes = monthStart ? overlapDays(p.start, p.end, monthStart, monthEnd) : p.dias;
  }

  filtered.sort((a,b)=> (a.gdo.localeCompare(b.gdo) || a.residente.localeCompare(b.residente) || a.start-b.start));

  const tbl = document.getElementById("vacTbl");
  const head = `<tr>
    <th style="min-width:220px;">Residente</th>
    <th>Gdo</th>
    <th>#</th>
    <th>Inicio</th>
    <th>Fin</th>
    <th>Días</th>
    ${monthStart ? '<th>Días (mes)</th>' : ''}
    <th>Fuente</th>
  </tr>`;
  const rows = filtered.map(p=>{
    const srcBadge = p.source==="matriz" ? '<span class="badge b-ok">Matriz</span>' : (p.source==="manual" ? '<span class="badge b-warn">Manual</span>' : '<span class="badge b-info">Mixto</span>');
    return `<tr>
      <td>${p.residente}</td>
      <td><span class="badge b-info">${p.gdo||"—"}</span></td>
      <td><span class="badge">${p.periodo}</span></td>
      <td>${fmtDateES(p.start)}</td>
      <td>${fmtDateES(p.end)}</td>
      <td><b>${p.dias}</b></td>
      ${monthStart ? '<td><b>'+p.dias_en_mes+'</b></td>' : ''}
      <td>${srcBadge}</td>
    </tr>`;
  }).join("");
  tbl.innerHTML = head + rows;
  return filtered;
}

// ------------------- Orchestrator -------------------
function refreshAll(){
  const periods = buildVacationPeriods(parseInt(startYear.value,10));
  updateKPIs(periods);

  // Reducir carga inicial: solo renderiza lo que está visible (tab activo).
  const active = (document.querySelector(".tabbtn.active")||{dataset:{tab:"op"}}).dataset.tab;

  if(active==="op"){
    renderStatus(periods);
  } else if(active==="rot"){
    renderMatrix();
    updateRotChart();
  } else if(active==="vac"){
    renderVacations(periods);
    updateVacChart(periods);
  } else if(active==="cov"){
    renderCoverage(periods);
  } else if(active==="imp"){
    renderImportValidation();
  }

  // ------------------- Exports -------------------
  document.getElementById("exportMatrix").onclick = ()=> {
    const out = getFilteredResidents().map(r=>{
      const o={Residente:r.Residente, Gdo:r.Gdo};
      for(const m of MONTHS) o[m]=getCell(r.Residente, m);
      return o;
    });
    download("matriz_rotaciones.csv", makeCSV(out));
  };

  document.getElementById("exportStatus").onclick = ()=> {
    const sy = parseInt(startYear.value,10);
    const dRef = parseISO(refDate.value);
    const mesSelVal = mesSel.value;
    const mes = (mesSelVal!=="ALL") ? mesSelVal : monthCodeFromDate(dRef);
    const yMes = cycleYearForMonthCode(mes, sy);
    const mNum = MONTH_TO_NUM[mes];
    const d = (mesSelVal!=="ALL") ? new Date(yMes, mNum-1, 15) : dRef;

    const monthStart = new Date(yMes, mNum-1, 1);
    const monthEnd = new Date(yMes, mNum, 0);

    const nextMes = nextMonthCode(mes);

    const out = getFilteredResidents().map(r=>{
      const rot = getRotation(r.Residente, mes) || "";
      const rotNext = nextMes ? (getRotation(r.Residente, nextMes)||"") : "";

      let ubic = rot;
      let estatus = "Activo";
      let ini=""; let fin="";

      if(mesSelVal!=="ALL"){
        const overlaps = periods
          .filter(p => p.residente===r.Residente && overlapDays(p.start, p.end, monthStart, monthEnd)>0)
          .sort((a,b)=>a.start-b.start);

        if(overlaps.length){
          const segs = overlaps.map(p=>{
            const s = (p.start < monthStart) ? monthStart : p.start;
            const e = (p.end > monthEnd) ? monthEnd : p.end;
            return {s,e};
          });

          estatus = "Vacaciones: " + segs.map(x=>{
            const ds = String(x.s.getDate()).padStart(2,"0");
            const de = String(x.e.getDate()).padStart(2,"0");
            return `${ds}-${de}`;
          }).join("; ");

          ini = segs.map(x=>toISO(x.s)).join("; ");
          fin = segs.map(x=>toISO(x.e)).join("; ");
        } else {
          const pNext = nextVacation(periods, r.Residente, d);
          if(pNext) estatus = "Proximas desde " + toISO(pNext.start);
        }
      } else {
        const pNow = isOnVacation(periods, r.Residente, d);
        const pNext = nextVacation(periods, r.Residente, d);
        if(pNow){
          ubic = "VACACIONES";
          estatus = "En vacaciones";
          ini = toISO(pNow.start);
          fin = toISO(pNow.end);
        } else if(pNext){
          estatus = "Proximas desde " + toISO(pNext.start);
        }
      }

      return {
        Residente: r.Residente,
        Gdo: r.Gdo,
        Mes: mes,
        Rotacion_programada: rot,
        Ubicacion_actual: ubic,
        Proxima_rotacion: rotNext,
        Estatus_vacacional: estatus,
        Inicio: ini,
        Fin: fin
      };
    });

    download("estado_operativo.csv", makeCSV(out));
  };

  document.getElementById("exportVac").onclick = ()=> {
    // calcular al momento del export para evitar depender del tab activo
    const vacFiltered = renderVacations(periods);
    const mesFiltro = mesSel.value;
    const out = vacFiltered.map(p=>({
      Residente: p.residente,
      Gdo: p.gdo,
      Periodo: p.periodo,
      Inicio: toISO(p.start),
      Fin: toISO(p.end),
      Dias_total: p.dias,
      Dias_en_mes: (p.dias_en_mes!==undefined ? p.dias_en_mes : p.dias),
      Mes_filtro: mesFiltro,
      Fuente: p.source
    }));
    download("vacaciones_filtradas.csv", makeCSV(out));
  };
}



// =================== COBERTURA (Alertas) ===================
let covState = { month: null, grade: null, base: "HEMCNR" };
let covProposal = []; // {Mes, Grado, Deficit, Candidato, Origen, Vacaciones}

function rotMatchesBase(rot, base){
  const r = (rot||"").toUpperCase();
  let b = (base||"").toUpperCase();
  if(!b) return false;
  // equivalencias típicas
  if(b==="HESCMNR") b="HEMCNR";
  if(b==="HEMCNR" && r.includes("HESCMNR")) return true;
  return r.includes(b);
}
function isEligibleOrigin(rot, mode){
  const r = (rot||"").toUpperCase();
  if(mode==="TODOS") return true;
  return r.startsWith("HGZ") || r.startsWith("HGR");
}
function uniqRotationsNoVac(){
  const s = new Set();
  for(const row of SCHEDULE_ROWS){
    for(const m of MONTHS){
      const r = getRotation(row.Residente, m);
      if(r) s.add(r);
    }
  }
  return Array.from(s).sort((a,b)=>a.localeCompare(b));
}
function loadReq(){
  const def = {R3:5, R4:4, R5:4}; // defaults: mediana práctica del plan actual
  try{
    const saved = JSON.parse(localStorage.getItem("cov_req_v1")||"null");
    if(saved && typeof saved==="object"){
      return {R3: +saved.R3 || def.R3, R4: +saved.R4 || def.R4, R5: +saved.R5 || def.R5};
    }
  } catch(e){}
  return def;
}
function saveReq(req){
  localStorage.setItem("cov_req_v1", JSON.stringify(req));
}

// ---- Objetivos por MES (Planeación) ----
function loadReqByMonth(){
  const def = {R3:5, R4:4, R5:4};
  try{
    const raw = localStorage.getItem("cov_req_by_month_v1");
    if(raw){
      const obj = JSON.parse(raw);
      return obj && typeof obj==="object" ? obj : {DEFAULT:def};
    }
  }catch(e){}
  // Seed con DEFAULT (compatibilidad con el req global)
  const global = loadReq();
  return {DEFAULT: {R3:+global.R3||def.R3, R4:+global.R4||def.R4, R5:+global.R5||def.R5}};
}
function getReqForMonth(mes){
  const def = {R3:5, R4:4, R5:4};
  const map = loadReqByMonth();
  const m = (mes && map[mes]) ? map[mes] : (map.DEFAULT || def);
  return {R3:+m.R3||def.R3, R4:+m.R4||def.R4, R5:+m.R5||def.R5};
}
function saveReqForMonth(mes, req){
  const map = loadReqByMonth();
  const key = mes || "DEFAULT";
  map[key] = {R3:+req.R3||0, R4:+req.R4||0, R5:+req.R5||0};
  // Mantén DEFAULT como el último guardado (comportamiento esperado)
  map.DEFAULT = map[key];
  localStorage.setItem("cov_req_by_month_v1", JSON.stringify(map));
  // Compatibilidad: también persiste el global
  saveReq(map.DEFAULT);
}
function setReqUIForMonth(mes){
  const r = getReqForMonth(mes);
  const r3 = document.getElementById("reqR3");
  const r4 = document.getElementById("reqR4");
  const r5 = document.getElementById("reqR5");
  if(r3) r3.value = r.R3;
  if(r4) r4.value = r.R4;
  if(r5) r5.value = r.R5;
  const pill = document.getElementById("objMesPill");
  if(pill) pill.textContent = "Mes: " + (mes || "—");
  covState.reqMonthLoaded = mes;
}
function ensureCoverageControls(){
  const baseSel = document.getElementById("covBase");
  if(baseSel && baseSel.options.length===0){
    const opts = ["HEMCNR", ...uniqRotationsNoVac().filter(x=>x!=="HEMCNR")];
    baseSel.innerHTML = opts.map(x=>`<option value="${x}">${x}</option>`).join("");
    baseSel.value = (covState.base && opts.includes(covState.base)) ? covState.base : "HEMCNR";
  }
  // (Planeación) Los objetivos se cargan por mes vía setReqUIForMonth().
  const r3 = document.getElementById("reqR3");
  const r4 = document.getElementById("reqR4");
  const r5 = document.getElementById("reqR5");
// Mes de análisis (independiente). Solo sugiere el mes global la primera vez.
  const covMes = document.getElementById("covMes");
  if(covMes){
    const globalMes = document.getElementById("mesSel")?.value || "ALL";
    if(!covState.month){
      covState.month = (globalMes!=="ALL" ? globalMes : (covMes.value || MONTHS[0]));
    }
    // Respeta la selección del usuario (no sobreescribir en cada refresh)
    covMes.value = covState.month;
    // Carga objetivos del mes (si cambia)
    if(covState.reqMonthLoaded !== covState.month){
      setReqUIForMonth(covState.month);
    } else {
      const pill = document.getElementById("objMesPill");
      if(pill) pill.textContent = "Mes: " + covState.month;
    }
  }
}
function getReqFromUI(){
  return {
    R3: +(document.getElementById("reqR3")?.value || 0),
    R4: +(document.getElementById("reqR4")?.value || 0),
    R5: +(document.getElementById("reqR5")?.value || 0),
  };
}
function countBaseByMonth(base, mes){
  const c = {R3:0, R4:0, R5:0, TOTAL:0};
  for(const row of SCHEDULE_ROWS){
    const g = row.Gdo;
    if(!(g in c)) continue;
    const rot = getRotation(row.Residente, mes);
    if(rotMatchesBase(rot, base)){
      c[g]++; c.TOTAL++;
    }
  }
  return c;
}
function computeCoverageTableByMonth(base){
  const rows=[];
  for(const mes of MONTHS){
    const req = getReqForMonth(mes);
    const c = countBaseByMonth(base, mes);
    rows.push({
      Mes: mes,
      R3: c.R3, R3_req: req.R3, R3_delta: c.R3-req.R3,
      R4: c.R4, R4_req: req.R4, R4_delta: c.R4-req.R4,
      R5: c.R5, R5_req: req.R5, R5_delta: c.R5-req.R5,
      TOTAL: c.TOTAL
    });
  }
  return rows;
}
function getVacInfoForMonth(periods, residente, mes, startYear){
  const y = cycleYearForMonthCode(mes, startYear);
  const mNum = MONTH_TO_NUM[mes];
  const ms = new Date(y, mNum-1, 1);
  const me = new Date(y, mNum, 0);
  const oneDay = 24*60*60*1000;
  let days=0;
  const ranges=[];
  for(const p of periods){
    if(p.residente!==residente) continue;
    const s = (p.start>ms)? p.start : ms;
    const e = (p.end<me)? p.end : me;
    if(e < s) continue;
    const d = Math.floor((e - s)/oneDay) + 1;
    days += d;
    ranges.push(String(s.getDate()).padStart(2,"0")+"-"+String(e.getDate()).padStart(2,"0"));
  }
  return {days, ranges};
}
function renderCoverage(periods){
  ensureCoverageControls();
  const baseSel = document.getElementById("covBase");
  const covMesSel = document.getElementById("covMes");
  const origenSel = document.getElementById("covOrigen");
  const startYear = parseInt(document.getElementById("startYear").value, 10);

  const base = baseSel?.value || "HEMCNR";
  covState.base = base;
  const mesSel = (covMesSel && covMesSel.value) ? covMesSel.value : (covState.month||MONTHS[0]);
  covState.month = mesSel;
  const reqSel = getReqForMonth(mesSel);

  // hint: tamaño de cohorte estimado según fracción de tiempo en sede base (observada)
  const cohorte = estimateCohorteSize(base, reqSel);
  const hint = document.getElementById("covCohorteHint");
  if(hint){
    hint.innerHTML = `<b>Estimación de cohorte (si mantienes la misma proporción de meses en sede base):</b> 
      R3≈<span class="mono">${cohorte.R3}</span>, R4≈<span class="mono">${cohorte.R4}</span>, R5≈<span class="mono">${cohorte.R5}</span> residentes (para cumplir el objetivo mensual).`;
  }

  const tableRows = computeCoverageTableByMonth(base);

  const tbl = document.getElementById("covTable");
  if(tbl){
    tbl.innerHTML = `
      <tr>
        <th>Mes</th>
        <th>R3 (real/obj/Δ)</th>
        <th>R4 (real/obj/Δ)</th>
        <th>R5 (real/obj/Δ)</th>
        <th>Total</th>
      </tr>
      ${tableRows.map(r=>{
        const c3 = covCellHTML("R3", r.Mes, r.R3, r.R3_req, r.R3_delta);
        const c4 = covCellHTML("R4", r.Mes, r.R4, r.R4_req, r.R4_delta);
        const c5 = covCellHTML("R5", r.Mes, r.R5, r.R5_req, r.R5_delta);
        return `<tr>
          <td><b>${r.Mes}</b></td>
          <td>${c3}</td>
          <td>${c4}</td>
          <td>${c5}</td>
          <td><span class="pill">${r.TOTAL}</span></td>
        </tr>`;
      }).join("")}
    `;
    // bind clicks
    tbl.querySelectorAll("[data-cov]").forEach(el=>{
      el.addEventListener("click", ()=>{
        covState.month = el.getAttribute("data-mes");
        covState.grade = el.getAttribute("data-gdo");
        if(covMesSel) covMesSel.value = covState.month;
        renderCoverageSide(periods);
      });
    });
  }

  // default selection
  if(!covState.month && covMesSel) covState.month = covMesSel.value;
  if(!covState.grade) covState.grade = "R3";
  if(covMesSel) covState.month = covMesSel.value;

  renderCoverageSide(periods);
}
function covCellHTML(gdo, mes, real, obj, delta){
  const bad = delta<0;
  const cls = bad ? "cellbtn bad" : "cellbtn good";
  const badge = bad
    ? `<span class="tag tag-red">FALTAN ${Math.abs(delta)}</span>`
    : `<span class="tag tag-blue">${delta===0?"OK":"+"+delta}</span>`;
  return `<span class="${cls}" data-cov="1" data-gdo="${gdo}" data-mes="${mes}">
    <b>${real}</b> / ${obj} &nbsp; ${badge}
  </span>`;
}
function estimateCohorteSize(base, reqOverride){
  // Estima N requerido = objetivo / (promedio meses en base / 12) por grado
  const req = reqOverride || getReqForMonth(covState.month||MONTHS[0]);
  const stats = {R3:{n:0, baseMonths:[]}, R4:{n:0, baseMonths:[]}, R5:{n:0, baseMonths:[]}};
  for(const row of SCHEDULE_ROWS){
    const g = row.Gdo;
    if(!(g in stats)) continue;
    stats[g].n++;
    let bm=0;
    for(const m of MONTHS){
      const rot = getRotation(row.Residente, m);
      if(rotMatchesBase(rot, base)) bm++;
    }
    stats[g].baseMonths.push(bm);
  }
  const out = {};
  for(const g of ["R3","R4","R5"]){
    const avgBM = stats[g].baseMonths.length ? stats[g].baseMonths.reduce((a,b)=>a+b,0)/stats[g].baseMonths.length : 0;
    const frac = avgBM ? (avgBM/12) : 0;
    out[g] = frac ? Math.ceil(req[g]/frac) : "—";
  }
  return out;
}
function renderCoverageSide(periods){
  const base = document.getElementById("covBase")?.value || "HEMCNR";
  const mes = document.getElementById("covMes")?.value || MONTHS[0];
  const origen = document.getElementById("covOrigen")?.value || "HGZ_HGR";
  const req = getReqFromUI();
  const startYear = parseInt(document.getElementById("startYear").value, 10);

  // Reco summary
  const c = countBaseByMonth(base, mes);
  const deficit = {
    R3: Math.max(0, req.R3 - c.R3),
    R4: Math.max(0, req.R4 - c.R4),
    R5: Math.max(0, req.R5 - c.R5),
  };
  const reco = document.getElementById("covReco");
  if(reco){
    reco.innerHTML = `
      <div class="item">
        <div><b>${base}</b> — <span class="tag tag-blue">${mes}</span></div>
        <div class="meta">Real: R3=${c.R3}, R4=${c.R4}, R5=${c.R5} · Objetivo: R3=${req.R3}, R4=${req.R4}, R5=${req.R5}</div>
        <div style="margin-top:8px; display:flex; gap:8px; flex-wrap:wrap;">
          ${deficit.R3?`<span class="tag tag-red">Déficit R3: ${deficit.R3}</span>`:`<span class="tag">R3 OK</span>`}
          ${deficit.R4?`<span class="tag tag-red">Déficit R4: ${deficit.R4}</span>`:`<span class="tag">R4 OK</span>`}
          ${deficit.R5?`<span class="tag tag-red">Déficit R5: ${deficit.R5}</span>`:`<span class="tag">R5 OK</span>`}
        </div>
      </div>
    `;
  }

  // Candidates (selected grade)
  const g = covState.grade || "R3";
  const need = deficit[g] || 0;

  const candBox = document.getElementById("covCandidates");
  const propBox = document.getElementById("covProposal");

  const y = cycleYearForMonthCode(mes, startYear);
  const mNum = MONTH_TO_NUM[mes];
  const ms = new Date(y, mNum-1, 1);
  const me = new Date(y, mNum, 0);

  const candidates = [];
  for(const row of SCHEDULE_ROWS){
    if(row.Gdo!==g) continue;
    const rot = getRotation(row.Residente, mes);
    if(rotMatchesBase(rot, base)) continue; // ya está en base
    if(!isEligibleOrigin(rot, origen)) continue;

    const vac = getVacInfoForMonth(periods, row.Residente, mes, startYear);
    // scoring simple: prefer HGZ/HGR, avoid vacations
    let score = 0;
    if(rot.toUpperCase().startsWith("HGZ") || rot.toUpperCase().startsWith("HGR")) score += 3;
    if(vac.days>0) score -= 3;
    // penaliza subs rotaciones típicamente rígidas
    if(["ONCO","PEDIA","ANDRO"].includes(rot.toUpperCase())) score -= 1;

    candidates.push({res: row.Residente, rot, vacDays: vac.days, vacRanges: vac.ranges, score});
  }
  candidates.sort((a,b)=>b.score-a.score || a.res.localeCompare(b.res));

  if(candBox){
    if(need===0){
      candBox.innerHTML = `<div class="note"><span class="tag">Sin déficit en ${g} para ${mes}</span></div>
      <div class="note" style="margin-top:8px;">Tip: ajusta objetivos o selecciona otra sede/mes.</div>`;
    } else {
      const top = candidates.slice(0, need);
      candBox.innerHTML = `
        <div class="note"><span class="tag tag-red">Faltan ${need} ${g}</span> · Origen: <span class="mono">${origen==="HGZ_HGR"?"HGZ/HGR":"Todos"}</span></div>
        <div class="list" style="margin-top:10px;">
          ${top.map(c=>candidateHTML(c, mes, g, true)).join("")}
        </div>
        <div class="note" style="margin-top:10px;">Otros candidatos (${Math.max(0,candidates.length-top.length)}):</div>
        <div class="list" style="margin-top:8px;">
          ${candidates.slice(top.length, top.length+12).map(c=>candidateHTML(c, mes, g, false)).join("") || '<div class="note">—</div>'}
        </div>
      `;
      candBox.querySelectorAll("[data-add-prop]").forEach(btn=>{
        btn.addEventListener("click", ()=>{
          const res = btn.getAttribute("data-res");
          const rot = btn.getAttribute("data-rot");
          addToProposal({Mes: mes, Grado: g, Deficit: need, Candidato: res, Origen: rot});
          renderProposal();
        });
      });
    }
  }

  renderProposal();
}
function candidateHTML(c, mes, gdo, primary){
  const vacTag = c.vacDays>0 ? `<span class="tag tag-red">VACACIONES ${c.vacRanges.join(", ")}</span>` : `<span class="tag">Sin V</span>`;
  const pri = primary ? `<span class="tag tag-warn">Prioridad</span>` : ``;
  return `<div class="item">
    <div style="display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;">
      <div><b>${c.res}</b> <span class="pill">${gdo}</span></div>
      <button class="btn" data-add-prop="1" data-res="${c.res}" data-rot="${c.rot}">Agregar a propuesta</button>
    </div>
    <div class="meta">Origen: <span class="mono">${c.rot}</span> · Score: <span class="mono">${c.score}</span></div>
    <div style="margin-top:8px; display:flex; gap:8px; flex-wrap:wrap;">
      ${pri} ${vacTag}
    </div>
  </div>`;
}
function addToProposal(p){
  // evita duplicados exactos Mes+Grado+Candidato
  const key = `${p.Mes}|${p.Grado}|${p.Candidato}`;
  if(covProposal.some(x=>`${x.Mes}|${x.Grado}|${x.Candidato}`===key)) return;
  covProposal.push(p);
}
function renderProposal(){
  const box = document.getElementById("covProposal");
  if(!box) return;
  if(covProposal.length===0){
    box.innerHTML = `<div class="note">Aún no hay propuesta. Usa “Agregar a propuesta”.</div>`;
    return;
  }
  box.innerHTML = `
    <div class="list">
      ${covProposal.map(p=>`<div class="item">
        <div style="display:flex; justify-content:space-between; gap:10px; flex-wrap:wrap;">
          <div><b>${p.Candidato}</b> <span class="pill">${p.Grado}</span> <span class="tag tag-blue">${p.Mes}</span></div>
          <button class="btn" data-del-prop="1" data-key="${p.Mes}|${p.Grado}|${p.Candidato}">Quitar</button>
        </div>
        <div class="meta">Origen: <span class="mono">${p.Origen}</span></div>
      </div>`).join("")}
    </div>
  `;
  box.querySelectorAll("[data-del-prop]").forEach(btn=>{
    btn.addEventListener("click", ()=>{
      const key = btn.getAttribute("data-key");
      covProposal = covProposal.filter(x=>`${x.Mes}|${x.Grado}|${x.Candidato}`!==key);
      renderProposal();
    });
  });
}

// =================== IMPORT ROTACIONES ===================
function parseCSV(text){
  const rows=[];
  let i=0, field="", row=[], inQ=false;
  const pushField=()=>{ row.push(field); field=""; };
  const pushRow=()=>{ if(row.length){ rows.push(row); } row=[]; };
  text = (text||"").replace(/\r\n/g,"\n").replace(/\r/g,"\n");
  while(i<text.length){
    const ch=text[i];
    if(inQ){
      if(ch === '"'){
        if(text[i+1]==='"'){ field+='"'; i++; }
        else { inQ=false; }
      } else { field+=ch; }
    } else {
      if(ch === '"'){ inQ=true; }
      else if(ch === ','){ pushField(); }
      else if(ch === '\n'){ pushField(); pushRow(); }
      else { field+=ch; }
    }
    i++;
  }
  pushField(); pushRow();
  // trim
  return rows.map(r=>r.map(x=>(x||"").trim())).filter(r=>r.some(x=>x!==""));
}
function emptyRow(){
  const o={Residente:"", Gdo:""};
  for(const m of MONTHS) o[m]="";
  return o;
}
function mergeSchedule(newRows){
  const map = new Map(SCHEDULE_ROWS.map(r=>[r.Residente, r]));
  for(const nr of newRows){
    const name = (nr.Residente||"").trim();
    if(!name) continue;
    let row = map.get(name);
    if(!row){
      row = emptyRow();
      row.Residente = name;
      row.Gdo = (nr.Gdo||"").trim();
      for(const m of MONTHS){ if(nr[m]) row[m]=String(nr[m]).trim(); }
      SCHEDULE_ROWS.push(row);
      map.set(name,row);
    } else {
      if(nr.Gdo) row.Gdo = String(nr.Gdo).trim();
      for(const m of MONTHS){
        if(nr[m]!==undefined && String(nr[m]).trim()!=="") row[m]=String(nr[m]).trim();
      }
    }
  }
  rebuildDerivedData();
  // persist
  localStorage.setItem("sched_rows_v1", JSON.stringify(SCHEDULE_ROWS));
}
function importWideCSV(text){
  const rows=parseCSV(text);
  if(rows.length<2) return {ok:false,msg:"CSV vacío o incompleto."};
  const header=rows[0].map(h=>norm(h));
  const idxRes=header.findIndex(h=>h==="residente");
  const idxGdo=header.findIndex(h=>h==="gdo"||h==="grado");
  if(idxRes<0) return {ok:false,msg:"No encuentro columna 'Residente'."};

  const monthIdx={};
  for(const m of MONTHS){
    const i = header.findIndex(h=>h===norm(m));
    if(i>=0) monthIdx[m]=i;
  }

  const newRows=[];
  for(const r of rows.slice(1)){
    const obj=emptyRow();
    obj.Residente = r[idxRes]||"";
    obj.Gdo = (idxGdo>=0)? (r[idxGdo]||"") : "";
    for(const m of MONTHS){
      const i = monthIdx[m];
      if(i!==undefined && i<r.length) obj[m]=r[i]||"";
    }
    newRows.push(obj);
  }
  mergeSchedule(newRows);
  return {ok:true,msg:`Importado formato ancho: ${newRows.length} filas (merge por Residente).`};
}
function importLongCSV(text){
  const rows=parseCSV(text);
  if(rows.length<2) return {ok:false,msg:"CSV vacío o incompleto."};
  const header=rows[0].map(h=>norm(h));
  const idxRes=header.findIndex(h=>h==="residente");
  const idxGdo=header.findIndex(h=>h==="gdo"||h==="grado");
  const idxMes=header.findIndex(h=>h==="mes");
  const idxRot=header.findIndex(h=>h==="rotacion"||h==="rotacion sede"||h==="rotacion/sede"||h==="rotacion_sede"||h==="sede");
  if(idxRes<0||idxMes<0||idxRot<0) return {ok:false,msg:"Requiero columnas: Residente, Mes, Rotacion (Gdo opcional)."};
  const newRowsMap=new Map();
  for(const r of rows.slice(1)){
    const name=(r[idxRes]||"").trim();
    if(!name) continue;
    const mes=(r[idxMes]||"").trim().toUpperCase();
    const rot=(r[idxRot]||"").trim();
    if(!MONTHS.includes(mes)) continue;
    let obj=newRowsMap.get(name);
    if(!obj){
      obj=emptyRow();
      obj.Residente=name;
      obj.Gdo=(idxGdo>=0)? (r[idxGdo]||"").trim() : "";
      newRowsMap.set(name,obj);
    }
    obj[mes]=rot;
  }
  const newRows=[...newRowsMap.values()];
  mergeSchedule(newRows);
  return {ok:true,msg:`Importado formato largo: ${newRows.length} residentes actualizados.`};
}
function renderImportValidation(){
  const box=document.getElementById("impValidation");
  if(!box) return;
  const issues=[];
  for(const row of SCHEDULE_ROWS){
    if(!row.Residente) continue;
    if(!row.Gdo) issues.push({tipo:"Grado vacío", residente: row.Residente});
    let emptyCount=0;
    for(const m of MONTHS){ if(!String(row[m]||"").trim()) emptyCount++; }
    if(emptyCount===12) issues.push({tipo:"Sin rotaciones (12 meses vacíos)", residente: row.Residente});
    if(MONTHS.some(m=>String(row[m]||"").toUpperCase().includes("PENDIENTE"))) issues.push({tipo:"PENDIENTE", residente: row.Residente});
  }
  if(issues.length===0){
    box.innerHTML = `<div class="note"><span class="tag">Sin alertas de validación</span></div>`;
    return;
  }
  box.innerHTML = `
    <div class="note"><span class="tag tag-warn">Alertas: ${issues.length}</span></div>
    <div style="overflow:auto; margin-top:8px;">
      <table>
        <tr><th>Tipo</th><th>Residente</th></tr>
        ${issues.slice(0,80).map(x=>`<tr><td>${x.tipo}</td><td>${x.residente}</td></tr>`).join("")}
      </table>
    </div>
  `;
}



// ------------------- Bindings: Cobertura + Import -------------------
(function bindCovAndImport(){
  const save = document.getElementById("saveReq");
  const reset = document.getElementById("resetReq");
  const exp = document.getElementById("exportCov");
  const expProp = document.getElementById("exportProposal");
  const clrProp = document.getElementById("clearProposal");
  const baseSel = document.getElementById("covBase");
  const mesSelCov = document.getElementById("covMes");
  const origenSel = document.getElementById("covOrigen");

  const planBtn = document.getElementById("planBtn");
  const objPanel = document.getElementById("covObjectivesPanel");
  const runBtn = document.getElementById("runCov");

  function hideObjectives(){ if(objPanel) objPanel.classList.add("hidden"); }
  function toggleObjectives(){ if(!objPanel) return; objPanel.classList.toggle("hidden"); if(!objPanel.classList.contains("hidden")){ setReqUIForMonth(document.getElementById("covMes")?.value || covState.month || MONTHS[0]); } }

  if(planBtn) planBtn.onclick = ()=>{ toggleObjectives(); };

  if(save) save.onclick = ()=>{
    const mes = document.getElementById("covMes")?.value || covState.month || MONTHS[0];
    saveReqForMonth(mes, getReqFromUI());
    toast("Objetivos guardados para " + mes + ". Recalculando…");
    hideObjectives();
    refreshAll();
  };
  if(reset) reset.onclick = ()=>{
    const mes = document.getElementById("covMes")?.value || covState.month || MONTHS[0];
    const def={R3:5,R4:4,R5:4};
    document.getElementById("reqR3").value=def.R3; document.getElementById("reqR4").value=def.R4; document.getElementById("reqR5").value=def.R5;
    saveReqForMonth(mes, def);
    toast("Objetivos reseteados para " + mes + ". Recalculando…");
    hideObjectives();
    refreshAll();
  };
  if(baseSel) baseSel.onchange = ()=>{ covState.base = baseSel.value; refreshAll(); };
  if(mesSelCov) mesSelCov.onchange = ()=>{ covState.month = mesSelCov.value; setReqUIForMonth(covState.month); refreshAll(); };
  if(origenSel) origenSel.onchange = ()=>refreshAll();
  if(runBtn) runBtn.onclick = ()=>{ covState.month = (mesSelCov?.value || covState.month || MONTHS[0]); covState.base = (baseSel?.value || covState.base || "HEMCNR"); refreshAll(); toast("Cobertura recalculada."); };

  if(exp) exp.onclick = ()=>{
    const base = document.getElementById("covBase")?.value || "HEMCNR";
    const rows = computeCoverageTableByMonth(base);
    download("cobertura_"+base+".csv", makeCSV(rows));
  };
  if(expProp) expProp.onclick = ()=>{ 
    if(!covProposal.length){ toast("No hay propuesta para exportar."); return; }
    download("propuesta_cobertura.csv", makeCSV(covProposal));
  };
  if(clrProp) clrProp.onclick = ()=>{ covProposal=[]; renderProposal(); toast("Propuesta limpiada."); };

  // Import UI
  const rotFile = document.getElementById("rotFile");
  const rotText = document.getElementById("rotText");
  const msg = document.getElementById("importMsg");
  const wideBtn = document.getElementById("importWide");
  const longBtn = document.getElementById("importLong");
  const tplBtn = document.getElementById("downloadTemplate");
  const expAll = document.getElementById("exportAllSchedule");
  const resetSched = document.getElementById("resetSchedule");
  const clearLocal = document.getElementById("clearScheduleLocal");

  if(rotFile) rotFile.addEventListener("change", (e)=>{
    const f = e.target.files && e.target.files[0];
    if(!f) return;
    const reader = new FileReader();
    reader.onload = ()=>{ if(rotText) rotText.value = String(reader.result||""); if(msg) msg.textContent = "Archivo cargado en el área de texto."; };
    reader.readAsText(f);
  });

  function setMsg(ok, t){
    if(!msg) return;
    msg.innerHTML = ok ? `<span class="tag tag-blue">OK</span> ${t}` : `<span class="tag tag-red">ERROR</span> ${t}`;
  }

  if(wideBtn) wideBtn.onclick = ()=>{
    const t = rotText?.value || "";
    const r = importWideCSV(t);
    setMsg(r.ok, r.msg);
    initControls();
    refreshAll();
  };
  if(longBtn) longBtn.onclick = ()=>{
    const t = rotText?.value || "";
    const r = importLongCSV(t);
    setMsg(r.ok, r.msg);
    initControls();
    refreshAll();
  };
  if(tplBtn) tplBtn.onclick = ()=>{
    const header = ["Residente","Gdo",...MONTHS];
    const sample = [header.join(","), "Apellido Nombre,R2,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE,PENDIENTE"].join("\n");
    download("plantilla_rotaciones.csv", sample);
  };
  if(expAll) expAll.onclick = ()=>{
    const out = SCHEDULE_ROWS.map(r=>{ const o={Residente:r.Residente, Gdo:r.Gdo}; for(const m of MONTHS) o[m]=r[m]||""; return o; });
    download("rotaciones_actuales.csv", makeCSV(out));
  };
  if(resetSched) resetSched.onclick = ()=>{
    SCHEDULE_ROWS = JSON.parse(JSON.stringify(BASE_SCHEDULE_ROWS));
    rebuildDerivedData();
    localStorage.setItem("sched_rows_v1", JSON.stringify(SCHEDULE_ROWS));
    initControls();
    refreshAll();
    setMsg(true, "Plan original restaurado.");
  };
  if(clearLocal) clearLocal.onclick = ()=>{
    localStorage.removeItem("sched_rows_v1");
    SCHEDULE_ROWS = JSON.parse(JSON.stringify(BASE_SCHEDULE_ROWS));
    rebuildDerivedData();
    initControls();
    refreshAll();
    setMsg(true, "Cambios locales borrados.");
  };
})();


// ------------------- Tabs -------------------
document.querySelectorAll(".tabbtn").forEach(btn=>{
  btn.addEventListener("click", ()=>{
    document.querySelectorAll(".tabbtn").forEach(b=>b.classList.remove("active"));
    document.querySelectorAll(".tabpanel").forEach(p=>p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("tab-"+btn.dataset.tab).classList.add("active");

    // Lazy render: al cambiar de pestaña, renderiza únicamente lo necesario.
    requestAnimationFrame(()=>refreshAll());
  });
});

// ------------------- Manual vacations -------------------
document.getElementById("addVacManual").onclick = ()=> {
  const v = document.getElementById("vacPaste").value.trim();
  if(!v) return;
  const parts = v.split(",").map(x=>x.trim());
  if(parts.length!==3){ alert("Formato esperado: Residente,YYYY-MM-DD,YYYY-MM-DD"); return; }
  const [res, s, e] = parts;
  if(!SCHEDULE_ROWS.some(r=>r.Residente===res)){ alert("Residente no encontrado en la matriz. Usa el mismo nombre que aparece en la matriz."); return; }
  const arr = loadManualVac();
  arr.push({residente:res, start:s, end:e});
  saveManualVac(arr);
  document.getElementById("vacPaste").value="";
  refreshAll();
};
document.getElementById("resetVacManual").onclick = ()=> {
  if(confirm("¿Borrar todos los periodos manuales guardados en este navegador?")){
    localStorage.removeItem("vac_manual");
    refreshAll();
  }
};

// ------------------- Listeners -------------------
["change","input"].forEach(ev=>{
  gdoSel.addEventListener(ev, refreshAll);
  mesSel.addEventListener(ev, refreshAll);
  document.getElementById("resSearch").addEventListener(ev, refreshAll);
  refDate.addEventListener(ev, refreshAll);
  startYear.addEventListener(ev, refreshAll);
});

// Init
initControls();
refreshAll();

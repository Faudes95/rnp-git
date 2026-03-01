/**
 * Patient Auto-fill Module — Item #8
 *
 * Auto-fills patient data from the master patient index when NSS is entered.
 * Works across ALL form templates (consulta, hospitalización, quirófano, urgencias).
 * Auto-attaches to any input with name="nss" or id containing "nss".
 */
(function(){
'use strict';

function cleanUrl(path){
  try{return new URL(path,window.location.origin).href}catch(e){return path}
}

// Standard field mappings (API response field -> possible form field names)
const FIELD_MAPPINGS = {
  'nombre': ['nombre','nombre_completo','paciente_nombre','nombre_paciente','nombre_pac','pac_nombre'],
  'edad': ['edad','edad_paciente','pac_edad'],
  'sexo': ['sexo','genero','sexo_paciente'],
  'curp': ['curp','curp_paciente'],
  'fecha_nacimiento': ['fecha_nacimiento','fecha_nac','fec_nac','nacimiento'],
  'tipo_sangre': ['tipo_sangre','grupo_sanguineo','tipo_sanguineo'],
  'telefono': ['telefono','tel','celular','telefono_paciente'],
  'email': ['email','correo','email_paciente'],
  'direccion': ['direccion','domicilio','direccion_paciente'],
  'estado_civil': ['estado_civil'],
  'ocupacion': ['ocupacion','profesion'],
  'escolaridad': ['escolaridad','nivel_educativo'],
  'umf': ['umf','unidad_medica','unidad_adscripcion'],
  'consultorio': ['consultorio'],
  'turno_adscripcion': ['turno_adscripcion','turno'],
  'delegacion': ['delegacion','delegacion_imss'],
  // Vital signs (from last visit)
  'peso': ['peso','peso_kg'],
  'talla': ['talla','talla_m'],
  'imc': ['imc'],
  'ta': ['ta','tension_arterial'],
  'fc': ['fc','frecuencia_cardiaca'],
  'temp': ['temp','temperatura'],
  // Clinical
  'diagnostico_principal': ['diagnostico_principal','dx_principal','diagnostico'],
  'alergias': ['alergias','alergias_conocidas'],
  'antecedentes': ['antecedentes_personales','app','antecedentes'],
};

// Set value in a form field by trying multiple possible names
function setField(fieldName, value, container){
  if(!value && value !== 0) return;
  const possibleNames = FIELD_MAPPINGS[fieldName] || [fieldName];
  for(let i=0; i<possibleNames.length; i++){
    const name = possibleNames[i];
    // Try by name
    let el = container.querySelector('[name="'+name+'"]');
    // Try by id
    if(!el) el = container.querySelector('#'+name);
    // Try by id containing
    if(!el) el = container.querySelector('[id*="'+name+'"]');
    if(el){
      if(el.tagName==='SELECT'){
        // Try to find matching option
        const opts = el.options;
        for(let j=0; j<opts.length; j++){
          if(opts[j].value.toUpperCase()===String(value).toUpperCase() ||
             opts[j].textContent.toUpperCase()===String(value).toUpperCase()){
            el.value = opts[j].value;
            el.dispatchEvent(new Event('change',{bubbles:true}));
            break;
          }
        }
      }else{
        el.value = value;
        el.dispatchEvent(new Event('input',{bubbles:true}));
        el.dispatchEvent(new Event('change',{bubbles:true}));
      }
      // Highlight filled field
      el.style.transition='background 0.5s';
      el.style.background='#e8f5e9';
      setTimeout(function(){el.style.background=''},2000);
      return;
    }
  }
}

// Show autofill notification
function showAutofillNotif(success, name){
  let notif=document.getElementById('rnp-autofill-notif');
  if(!notif){
    notif=document.createElement('div');
    notif.id='rnp-autofill-notif';
    notif.style.cssText='position:fixed;top:20px;left:50%;transform:translateX(-50%);z-index:9999;'+
      'padding:10px 20px;border-radius:10px;font-size:0.85rem;font-weight:500;'+
      'box-shadow:0 4px 16px rgba(0,0,0,0.15);transition:all 0.3s;font-family:Segoe UI,sans-serif';
    document.body.appendChild(notif);
  }
  if(success){
    notif.style.background='#e8f5e9';
    notif.style.color='#2e7d32';
    notif.style.border='1px solid #a5d6a7';
    notif.textContent='✅ Datos cargados: '+name;
  }else{
    notif.style.background='#fff3e0';
    notif.style.color='#e65100';
    notif.style.border='1px solid #ffcc80';
    notif.textContent='ℹ️ Paciente no encontrado — complete manualmente';
  }
  notif.style.display='block';
  setTimeout(function(){notif.style.display='none'},4000);
}

// Main autofill function
window.rnpAutofill=async function(nss, container){
  container=container||document;
  if(!nss||nss.length<10)return;

  try{
    const res=await fetch(cleanUrl('/api/patient/autofill?nss='+encodeURIComponent(nss)),{
      credentials:'same-origin'
    });
    const data=await res.json();

    if(!data.ok||!data.patient){
      showAutofillNotif(false,'');
      return;
    }

    const p=data.patient;

    // Set all fields
    Object.keys(FIELD_MAPPINGS).forEach(function(key){
      if(p[key]!==undefined && p[key]!==null){
        setField(key, p[key], container);
      }
    });

    // Also set any additional fields from patient data
    Object.keys(p).forEach(function(key){
      if(!FIELD_MAPPINGS[key]){
        setField(key, p[key], container);
      }
    });

    showAutofillNotif(true, p.nombre||p.nombre_completo||nss);

    // Store in sessionStorage for cross-page use
    sessionStorage.setItem('rnp_patient_'+nss, JSON.stringify(p));

  }catch(err){
    console.warn('Autofill error:',err);
    showAutofillNotif(false,'');
  }
};

// Auto-attach on DOMContentLoaded
document.addEventListener('DOMContentLoaded',function(){
  // Find NSS inputs
  const nssInputs=document.querySelectorAll(
    'input[name="nss"], input[id="nss"], input[name*="nss"], input[id*="nss"]'
  );

  nssInputs.forEach(function(input){
    // Add autofill button next to NSS input
    const wrapper=document.createElement('div');
    wrapper.style.cssText='display:inline-flex;align-items:center;gap:6px;width:100%';

    const btn=document.createElement('button');
    btn.type='button';
    btn.textContent='🔍';
    btn.title='Auto-llenar datos del paciente';
    btn.style.cssText='padding:6px 10px;border:1.5px solid #006b3f;background:#e8f5e9;'+
      'color:#006b3f;border-radius:8px;cursor:pointer;font-size:1rem;flex-shrink:0;transition:all 0.2s';
    btn.onmouseover=function(){btn.style.background='#c8e6c9'};
    btn.onmouseout=function(){btn.style.background='#e8f5e9'};
    btn.onclick=function(){
      window.rnpAutofill(input.value.trim(),input.closest('form')||document);
    };

    // Replace input with wrapper
    if(input.parentNode){
      input.parentNode.insertBefore(wrapper,input);
      wrapper.appendChild(input);
      wrapper.appendChild(btn);
      input.style.flex='1';
    }

    // Auto-fill on blur if 10 digits
    input.addEventListener('blur',function(){
      if(input.value.trim().length===10){
        window.rnpAutofill(input.value.trim(),input.closest('form')||document);
      }
    });
  });
});

})();

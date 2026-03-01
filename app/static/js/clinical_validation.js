/**
 * Clinical Validation Module — Item #3
 *
 * Validates form data before save by calling /api/clinical/validate.
 * Auto-attaches to all forms with class "clinical-form" or data-validate="true".
 * Shows inline errors/warnings without blocking user workflow.
 */
(function(){
'use strict';

function cleanUrl(path){
  try{return new URL(path,window.location.origin).href}catch(e){return path}
}

// Collect form data from standard field names
function collectFormData(form){
  const data={};
  const fields=form.querySelectorAll('input,select,textarea');
  fields.forEach(function(f){
    const name=f.name||f.id;
    if(!name)return;
    let val=f.value;
    if(f.type==='number'&&val)val=parseFloat(val);
    if(f.type==='checkbox')val=f.checked;
    if(val!==''&&val!==null&&val!==undefined)data[name]=val;
  });
  return data;
}

// Detect context from URL
function detectContext(){
  const p=window.location.pathname.toLowerCase();
  if(p.includes('quirofano'))return 'quirofano';
  if(p.includes('urgencia'))return 'urgencia';
  if(p.includes('hospitalizacion'))return 'hospitalizacion';
  return 'consulta';
}

// Create validation UI
function createValidationUI(){
  let container=document.getElementById('rnp-validation-panel');
  if(container)return container;
  container=document.createElement('div');
  container.id='rnp-validation-panel';
  container.style.cssText='position:fixed;bottom:20px;right:20px;z-index:9999;max-width:380px;'+
    'font-family:Segoe UI,system-ui,sans-serif;transition:all 0.3s;pointer-events:auto';
  document.body.appendChild(container);
  return container;
}

function showValidationResults(result){
  const panel=createValidationUI();
  if(!result||(!result.errors.length&&!result.warnings.length&&!result.info.length)){
    panel.innerHTML='<div style="background:#e8f5e9;border:1px solid #a5d6a7;color:#2e7d32;'+
      'padding:12px 16px;border-radius:12px;box-shadow:0 4px 16px rgba(0,0,0,0.12);'+
      'display:flex;align-items:center;gap:8px;font-size:0.85rem">'+
      '<span style="font-size:1.2rem">✅</span> Datos clínicos válidos</div>';
    setTimeout(function(){panel.innerHTML=''},4000);
    return true;
  }

  let html='<div style="background:white;border-radius:14px;box-shadow:0 8px 30px rgba(0,0,0,0.18);overflow:hidden;max-height:60vh;overflow-y:auto">';

  // Header
  const hasErrors=result.errors.length>0;
  html+='<div style="padding:12px 16px;background:'+(hasErrors?'#c62828':'#ff8f00')+
    ';color:white;display:flex;justify-content:space-between;align-items:center">'+
    '<strong>'+(hasErrors?'❌ Errores de validación':'⚠️ Advertencias')+'</strong>'+
    '<button onclick="this.closest(\'#rnp-validation-panel\').innerHTML=\'\'" '+
    'style="background:none;border:none;color:white;cursor:pointer;font-size:1.2rem">✕</button></div>';

  // Errors
  if(result.errors.length){
    html+='<div style="padding:10px 16px">';
    result.errors.forEach(function(e){
      html+='<div style="padding:6px 10px;margin:4px 0;background:#ffebee;border-left:3px solid #f44336;'+
        'border-radius:4px;font-size:0.82rem;color:#c62828">'+e+'</div>';
    });
    html+='</div>';
  }

  // Warnings
  if(result.warnings.length){
    html+='<div style="padding:10px 16px">';
    result.warnings.forEach(function(w){
      html+='<div style="padding:6px 10px;margin:4px 0;background:#fff3e0;border-left:3px solid #ff9800;'+
        'border-radius:4px;font-size:0.82rem;color:#e65100">'+w+'</div>';
    });
    html+='</div>';
  }

  // Info
  if(result.info&&result.info.length){
    html+='<div style="padding:10px 16px">';
    result.info.forEach(function(i){
      html+='<div style="padding:6px 10px;margin:4px 0;background:#e3f2fd;border-left:3px solid #2196f3;'+
        'border-radius:4px;font-size:0.82rem;color:#1565c0">ℹ️ '+i+'</div>';
    });
    html+='</div>';
  }

  html+='</div>';
  panel.innerHTML=html;

  // Auto-dismiss after 15s if only warnings
  if(!hasErrors){
    setTimeout(function(){if(panel.innerHTML)panel.innerHTML=''},15000);
  }

  return !hasErrors;
}

// Main validation function
window.rnpValidate=async function(formOrData, context){
  const data = formOrData instanceof HTMLFormElement ? collectFormData(formOrData) : formOrData;
  const ctx = context || detectContext();

  try{
    const res=await fetch(cleanUrl('/api/clinical/validate'),{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      credentials:'same-origin',
      body:JSON.stringify({data:data,context:ctx})
    });
    const result=await res.json();
    return showValidationResults(result);
  }catch(err){
    console.warn('Validation API error:',err);
    return true; // Don't block on API errors
  }
};

// Auto-attach to forms on page load
document.addEventListener('DOMContentLoaded',function(){
  // Find all forms with clinical-form class or data-validate attribute
  const forms=document.querySelectorAll('form.clinical-form, form[data-validate="true"]');
  forms.forEach(function(form){
    form.addEventListener('submit',function(e){
      // Only validate, don't prevent submit (async validation is best-effort)
      const data=collectFormData(form);
      window.rnpValidate(data);
    });
  });

  // Also add a validate button if there's a submit-bar
  const submitBars=document.querySelectorAll('.submit-bar');
  submitBars.forEach(function(bar){
    const btn=document.createElement('button');
    btn.type='button';
    btn.textContent='🔍 Validar';
    btn.style.cssText='padding:10px 20px;background:#fff3e0;color:#e65100;border:1.5px solid #ff9800;'+
      'border-radius:8px;cursor:pointer;font-weight:600;font-size:0.85rem;margin-right:10px;transition:all 0.2s';
    btn.onclick=function(){
      const form=bar.closest('form')||document;
      window.rnpValidate(collectFormData(form));
    };
    bar.insertBefore(btn,bar.firstChild);
  });
});

})();

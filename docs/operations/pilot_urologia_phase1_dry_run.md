# Fase 1 del Piloto `pilot_urologia`: Dry Run Interno

## Objetivo
- Validar técnicamente el corredor completo antes de abrir la ventana clínica supervisada.

## Participantes mínimos
- 1 responsable técnico
- 1 usuario de quirófano
- 1 usuario de jefatura

## Secuencia mínima
1. Ejecutar `npm run pilot:phase1:urologia`
2. Confirmar login y navegación base
3. Correr 5 casos controlados:
   - urgencia quirúrgica
   - programada
   - lista de espera
   - central/residentes
   - hospitalización/censo/expediente
4. Registrar toda incidencia reproducible

## Criterio de salida
- Cero bloqueos operativos severos
- Cero errores 500 repetibles
- Exportes y censo alineados

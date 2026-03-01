import { useEffect, useRef, useState } from "react";

export default function App() {
  const [logged, setLogged] = useState(false);
  const [patientNotes, setPatientNotes] = useState("");
  const [history, setHistory] = useState([]);
  const [toast, setToast] = useState("");
  const [insight, setInsight] = useState(
    "Ingrese una nota clínica para generar hallazgos."
  );
  const toastTimer = useRef(null);

  const showToast = (message) => {
    setToast(message);
    if (toastTimer.current) window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(""), 2600);
  };

  useEffect(() => {
    return () => {
      if (toastTimer.current) window.clearTimeout(toastTimer.current);
    };
  }, []);

  const normalize = (value) => value.toLowerCase();

  const detectEntities = (text) => {
    const entities = [];
    const note = normalize(text);

    const psaMatch = note.match(/psa\s*([0-9]+(?:[.,][0-9]+)?)/);
    if (note.includes("psa")) {
      entities.push(
        psaMatch ? `PSA ${psaMatch[1].replace(",", ".")}` : "PSA mencionado"
      );
    }

    const gleasonMatch = note.match(/gleason\s*([0-9]+\s*\+\s*[0-9]+)/);
    if (note.includes("gleason") || /\b\d\s*\+\s*\d\b/.test(note)) {
      entities.push(
        gleasonMatch
          ? `Gleason ${gleasonMatch[1].replace(/\s+/g, "")}`
          : "Gleason score"
      );
    }

    if (note.includes("metast") || /\bm1\b/.test(note)) {
      entities.push("Posible metástasis");
    }

    if (note.includes("ósea") || note.includes("osea") || note.includes("bone")) {
      entities.push("Compromiso óseo");
    }

    const stageMatch = note.match(/\bt\d[ab]?\b/);
    if (stageMatch) {
      entities.push(`Estadio ${stageMatch[0].toUpperCase()}`);
    }

    const nodeMatch = note.match(/\bn\d\b/);
    if (nodeMatch) {
      entities.push(`Ganglios ${nodeMatch[0].toUpperCase()}`);
    }

    return entities;
  };

  const buildInsight = (entities) => {
    if (entities.length === 0) {
      return "No se detectaron entidades clave. Agregue PSA, Gleason o estadificación.";
    }

    const highRisk = entities.some((entity) => entity.includes("metástasis"));
    const hasGleason = entities.some((entity) =>
      entity.toLowerCase().includes("gleason")
    );
    const hasPsa = entities.some((entity) =>
      entity.toLowerCase().includes("psa")
    );

    if (highRisk) {
      return "Riesgo alto: sugerir imágenes avanzadas y plan onco multidisciplinario.";
    }
    if (hasGleason && hasPsa) {
      return "Perfil intermedio-alto: evaluar tratamiento combinado y seguimiento estrecho.";
    }
    return "Hallazgos iniciales: completar estudio con imágenes y laboratorio.";
  };

  const handleAnalyze = () => {
    const note = patientNotes.trim();
    if (!note) {
      showToast("Ingrese una nota clínica antes de analizar.");
      return;
    }

    const entities = detectEntities(note);
    const nextInsight = buildInsight(entities);

    setHistory((prev) => [
      {
        note,
        entities: entities.length ? entities : ["Ninguna entidad detectada"],
        insight: nextInsight,
        date: new Date(),
      },
      ...prev,
    ]);

    setInsight(nextInsight);
    setPatientNotes("");
    showToast("Análisis completado. Entidades detectadas.");
  };

  const onKeyDown = (event) => {
    if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
      handleAnalyze();
    }
  };

  return (
    <div className="clinica-ia">
      <style>{`
        body { margin: 0; }
        .clinica-ia {
          min-height: 100vh;
          padding: 32px 20px 48px;
          font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
          color: #10151f;
          background: radial-gradient(circle at top, #f9fbff 0%, #eef1f6 45%, #e8ecf4 100%);
        }
        .clinica-ia * { box-sizing: border-box; }
        .hero {
          padding: 24px 28px;
          border-radius: 22px;
          background: linear-gradient(135deg, #ffffff 0%, #f7f1ea 40%, #eef6f4 100%);
          border: 1px solid rgba(13, 138, 122, 0.15);
          box-shadow: 0 20px 45px rgba(16, 21, 31, 0.12);
          display: grid;
          gap: 12px;
          margin-bottom: 28px;
        }
        .hero h1 {
          margin: 0;
          font-family: "Fraunces", "Georgia", serif;
          font-size: clamp(1.8rem, 2.3vw, 2.6rem);
          letter-spacing: -0.5px;
        }
        .hero p {
          margin: 0;
          color: #5b6678;
          font-size: 1rem;
          max-width: 780px;
        }
        .chips { display: flex; flex-wrap: wrap; gap: 8px; }
        .chip {
          padding: 6px 12px;
          border-radius: 999px;
          font-size: 0.82rem;
          background: rgba(13, 138, 122, 0.1);
          color: #0d8a7a;
          border: 1px solid rgba(13, 138, 122, 0.2);
        }
        .chip.warning {
          background: rgba(255, 143, 63, 0.12);
          color: #a34910;
          border-color: rgba(255, 143, 63, 0.4);
        }
        .card {
          background: #ffffff;
          border-radius: 16px;
          border: 1px solid #d6dde8;
          box-shadow: 0 18px 40px rgba(16, 21, 31, 0.08);
          padding: 22px;
        }
        .card h2 { margin: 0 0 14px; font-size: 1.2rem; }
        .login {
          max-width: 420px;
          margin: 0 auto 32px;
          animation: rise 0.5s ease forwards;
        }
        .field { display: grid; gap: 8px; margin-bottom: 14px; }
        label { font-size: 0.9rem; color: #5b6678; }
        input, textarea {
          font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
          padding: 12px 14px;
          border-radius: 12px;
          border: 1px solid #d6dde8;
          background: #fbfcff;
          font-size: 0.95rem;
        }
        textarea { min-height: 130px; resize: vertical; }
        button {
          font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
          font-weight: 600;
          border: none;
          border-radius: 999px;
          padding: 12px 18px;
          cursor: pointer;
          background: #0d8a7a;
          color: white;
          transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        button:hover {
          transform: translateY(-1px);
          box-shadow: 0 10px 22px rgba(13, 138, 122, 0.22);
        }
        .stage { display: grid; gap: 22px; }
        .grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 22px;
          align-items: start;
        }
        .split { display: grid; gap: 14px; }
        .insight {
          background: #f7faf9;
          border: 1px solid rgba(13, 138, 122, 0.18);
          padding: 12px 14px;
          border-radius: 14px;
          font-size: 0.92rem;
        }
        .history-item {
          padding: 14px;
          border-radius: 14px;
          border: 1px solid rgba(16, 21, 31, 0.08);
          background: #fafbff;
          display: grid;
          gap: 8px;
          animation: fade 0.4s ease;
        }
        .history-meta { font-size: 0.78rem; color: #5b6678; }
        .history-tags { display: flex; gap: 8px; flex-wrap: wrap; }
        .tag {
          padding: 4px 8px;
          background: rgba(13, 138, 122, 0.1);
          color: #0d8a7a;
          border-radius: 999px;
          font-size: 0.75rem;
        }
        .tag.alt {
          background: rgba(255, 143, 63, 0.2);
          color: #a34910;
        }
        .empty {
          color: #5b6678;
          font-size: 0.95rem;
          text-align: center;
          padding: 18px;
          border-radius: 12px;
          border: 1px dashed rgba(16, 21, 31, 0.2);
        }
        .toast {
          margin-top: 12px;
          font-size: 0.88rem;
          color: #1f2937;
          background: #eef6f4;
          border: 1px solid rgba(13, 138, 122, 0.2);
          padding: 10px 12px;
          border-radius: 10px;
        }
        .muted { color: #5b6678; font-size: 0.9rem; }
        @keyframes rise {
          from { opacity: 0; transform: translateY(16px); }
          to { opacity: 1; transform: translateY(0); }
        }
        @keyframes fade { from { opacity: 0; } to { opacity: 1; } }
        @media (max-width: 980px) { .grid { grid-template-columns: 1fr; } }
        @media (prefers-reduced-motion: reduce) {
          .clinica-ia * { animation: none !important; transition: none !important; }
        }
      `}</style>

      <header className="hero">
        <h1>Plataforma Clínica IA</h1>
        <p>
          Vista preliminar para ingreso clínico, extracción de entidades y seguimiento longitudinal.
          Esta simulación no almacena datos reales.
        </p>
        <div className="chips">
          <span className="chip">Oncología urológica</span>
          <span className="chip">IA asistiva</span>
          <span className="chip warning">Modo demo</span>
        </div>
      </header>

      {!logged ? (
        <section className="card login">
          <h2>Ingreso médico</h2>
          <div className="field">
            <label htmlFor="user">Usuario</label>
            <input id="user" type="text" placeholder="medico@hospital.org" />
          </div>
          <div className="field">
            <label htmlFor="pass">Contraseña</label>
            <input id="pass" type="password" placeholder="********" />
          </div>
          <button type="button" onClick={() => setLogged(true)}>
            Ingresar
          </button>
          <p className="muted">Acceso simulado. Se habilita solo la vista de la plataforma.</p>
        </section>
      ) : (
        <main className="stage">
          <section className="grid">
            <div className="card">
              <h2>Ingreso clínico</h2>
              <div className="field">
                <label htmlFor="notes">Notas del paciente</label>
                <textarea
                  id="notes"
                  value={patientNotes}
                  onChange={(event) => setPatientNotes(event.target.value)}
                  onKeyDown={onKeyDown}
                  placeholder="Ej. PSA 45 ng/ml, Gleason 4+4, metástasis ósea, T3b N1"
                />
              </div>
              <div className="split">
                <button type="button" onClick={handleAnalyze}>
                  Analizar con IA
                </button>
                <div className="insight">
                  <strong>Insight rápido</strong>
                  <div className="muted">{insight}</div>
                </div>
                {toast && <div className="toast">{toast}</div>}
              </div>
            </div>

            <div className="card">
              <h2>Historial longitudinal</h2>
              <div className="split">
                {history.length === 0 ? (
                  <div className="empty">Sin registros aún. El primer análisis aparecerá aquí.</div>
                ) : (
                  history.map((item, index) => (
                    <div key={index} className="history-item">
                      <div className="history-meta">
                        Registro {item.date.toLocaleString("es-ES")}
                      </div>
                      <div>{item.note}</div>
                      <div className="history-tags">
                        {item.entities.map((entity, entityIndex) => (
                          <span
                            key={entityIndex}
                            className={entity.toLowerCase().includes("metástasis") ? "tag alt" : "tag"}
                          >
                            {entity}
                          </span>
                        ))}
                      </div>
                      <div className="muted">Insight: {item.insight}</div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </section>
        </main>
      )}
    </div>
  );
}

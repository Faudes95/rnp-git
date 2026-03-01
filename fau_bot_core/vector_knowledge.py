from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

from .schema import schema_name
from .time_utils import utcnow

try:
    from embeddings import compute_embedding
except Exception:
    compute_embedding = None

_CHUNK_SIZE = max(300, int(os.getenv("FAU_CORE_KNOWLEDGE_CHUNK_SIZE", "900") or 900))
_CHUNK_OVERLAP = max(40, int(os.getenv("FAU_CORE_KNOWLEDGE_CHUNK_OVERLAP", "120") or 120))
_CANDIDATE_FACTOR = max(3, int(os.getenv("FAU_CORE_KNOWLEDGE_CANDIDATE_FACTOR", "8") or 8))
_MAX_CANDIDATES = max(50, int(os.getenv("FAU_CORE_KNOWLEDGE_MAX_CANDIDATES", "300") or 300))
_CHUNK_SUFFIX_RE = re.compile(r"\s*\[chunk\s+\d+/\d+\]\s*$", re.IGNORECASE)
_WORD_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)
_CLINICAL_HEADER_RE = re.compile(
    r"^\s*(dx|diagnostico|diagnostico principal|impresion|analisis|plan|manejo|tratamiento|laboratorios|hallazgos|complicaciones|seguimiento)\s*[:\-]\s*$",
    re.IGNORECASE,
)

# Expansiones clínicas para mejorar recuperación semántica/lexical híbrida.
_QUERY_EXPANSIONS = {
    "litos": ["litiasis", "calculo", "renal", "ureter", "vejiga"],
    "calculo": ["litiasis", "lito", "urolitiasis"],
    "jj": ["cateter", "doble", "j", "stent", "ureteral"],
    "prostata": ["prostatico", "gleason", "isup", "psa", "ape"],
    "vejiga": ["urotelial", "rtu", "cistectomia"],
    "renal": ["rinon", "nefrectomia", "tumor", "riñon"],
    "sepsis": ["infeccion", "obstruccion", "cultivos", "antibiotico"],
    "sangrado": ["hemorragia", "transfusion", "hemoderivados"],
    "charlson": ["comorbilidad", "comorbilidades"],
    "ecog": ["funcional", "performance"],
}


def _fallback_embedding(text_value: str, dim: int = 384) -> List[float]:
    vec = [0.0] * dim
    for token in (text_value or "").lower().split():
        h = int(hashlib.sha256(token.encode("utf-8")).hexdigest(), 16)
        pos = h % dim
        vec[pos] += 1.0
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        return vec
    return [x / norm for x in vec]


def build_embedding(text_value: str) -> List[float]:
    if os.getenv("FAU_CORE_USE_FALLBACK_EMBEDDING", "false").lower() in ("1", "true", "yes"):
        return _fallback_embedding(text_value)
    if compute_embedding is not None:
        try:
            out = compute_embedding(text_value)
            if out and isinstance(out, list):
                return [float(x) for x in out]
        except Exception:
            pass
    return _fallback_embedding(text_value)


def _vec_literal(values: List[float]) -> str:
    return "[" + ",".join(f"{float(v):.8f}" for v in values) + "]"


def _is_postgres(conn: Connection) -> bool:
    try:
        return str(conn.engine.dialect.name).startswith("postgres")
    except Exception:
        return False


def _is_sqlite(conn: Connection) -> bool:
    try:
        return str(conn.engine.dialect.name).startswith("sqlite")
    except Exception:
        return False


def _strip_accents(text_value: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", str(text_value or "")) if not unicodedata.combining(ch))


def _norm(text_value: str) -> str:
    return _strip_accents(str(text_value or "")).lower()


def _tokenize(text_value: str) -> List[str]:
    return _WORD_RE.findall(_norm(text_value))


def _base_title(title: str) -> str:
    return _CHUNK_SUFFIX_RE.sub("", str(title or "")).strip()


def _split_sentences(paragraph: str) -> List[str]:
    parts = re.split(r"(?<=[\.\!\?])\s+", str(paragraph or "").strip())
    out = [p.strip() for p in parts if p.strip()]
    if out:
        return out
    raw = str(paragraph or "").strip()
    return [raw] if raw else []


def _split_clinical_sections(content: str) -> List[str]:
    lines = [ln.rstrip() for ln in str(content or "").splitlines()]
    sections: List[str] = []
    current_title = ""
    buf: List[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if buf:
                section_body = "\n".join(buf).strip()
                if section_body:
                    sections.append(f"{current_title}\n{section_body}".strip())
                buf = []
            continue
        if _CLINICAL_HEADER_RE.match(line):
            if buf:
                section_body = "\n".join(buf).strip()
                if section_body:
                    sections.append(f"{current_title}\n{section_body}".strip())
                buf = []
            current_title = line
            continue
        buf.append(line)
    if buf:
        section_body = "\n".join(buf).strip()
        if section_body:
            sections.append(f"{current_title}\n{section_body}".strip())
    return [s for s in sections if s.strip()]


def _chunk_content(content: str, *, max_chars: int = _CHUNK_SIZE, overlap_chars: int = _CHUNK_OVERLAP) -> List[str]:
    text_value = str(content or "").strip()
    if not text_value:
        return []
    if len(text_value) <= max_chars:
        return [text_value]

    sections = _split_clinical_sections(text_value)
    if sections:
        paragraphs = sections
    else:
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text_value) if p.strip()]
    if not paragraphs:
        paragraphs = [text_value]

    chunks: List[str] = []
    buffer = ""
    for paragraph in paragraphs:
        for sentence in _split_sentences(paragraph):
            candidate = (buffer + " " + sentence).strip() if buffer else sentence
            if len(candidate) <= max_chars:
                buffer = candidate
                continue
            if buffer:
                chunks.append(buffer)
            # Si una oración supera el máximo, se trocea duro.
            if len(sentence) > max_chars:
                start = 0
                while start < len(sentence):
                    end = min(len(sentence), start + max_chars)
                    piece = sentence[start:end].strip()
                    if piece:
                        chunks.append(piece)
                    if end >= len(sentence):
                        break
                    start = max(0, end - overlap_chars)
                buffer = ""
            else:
                buffer = sentence
    if buffer:
        chunks.append(buffer)

    # Overlap suave entre chunks para no perder contexto clínico.
    if len(chunks) <= 1:
        return chunks
    enhanced: List[str] = []
    for idx, chunk in enumerate(chunks):
        if idx == 0:
            enhanced.append(chunk)
            continue
        prev_tail = chunks[idx - 1][-overlap_chars:].strip()
        joined = (prev_tail + " " + chunk).strip() if prev_tail else chunk
        enhanced.append(joined[: max_chars + overlap_chars])
    return enhanced


def _expanded_query_terms(query: str) -> List[str]:
    raw_tokens = _tokenize(query)
    expanded = list(raw_tokens)
    for token in raw_tokens:
        expanded.extend(_QUERY_EXPANSIONS.get(token, []))
    # dedupe preservando orden
    out: List[str] = []
    seen = set()
    for token in expanded:
        if len(token) < 2:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _lexical_score(query_terms: List[str], title: str, content: str, tags: Optional[List[str]] = None) -> float:
    if not query_terms:
        return 0.0
    tags_text = " ".join(tags or [])
    full_text = _norm(f"{title} {content} {tags_text}")
    doc_tokens = set(_tokenize(full_text))
    if not doc_tokens:
        return 0.0

    hits = 0
    phrase_hits = 0
    for token in query_terms:
        if token in doc_tokens:
            hits += 1
            if token in full_text:
                phrase_hits += 1

    coverage = float(hits) / float(len(query_terms) or 1)
    phrase = float(phrase_hits) / float(len(query_terms) or 1)
    return min(1.0, (0.8 * coverage) + (0.2 * phrase))


def _bigram_tokens(tokens: List[str]) -> List[str]:
    if len(tokens) < 2:
        return []
    return [f"{tokens[i]} {tokens[i + 1]}" for i in range(0, len(tokens) - 1)]


def _rerank_score(query_text: str, query_terms: List[str], title: str, content: str) -> float:
    norm_query = _norm(query_text)
    full_text = _norm(f"{title} {content}")
    if not norm_query or not full_text:
        return 0.0

    phrase_boost = 1.0 if norm_query in full_text and len(norm_query) >= 8 else 0.0
    q_tokens = [t for t in _tokenize(norm_query) if len(t) >= 3]
    d_tokens = [t for t in _tokenize(full_text) if len(t) >= 3]
    if not q_tokens or not d_tokens:
        return 0.25 * phrase_boost

    q_bigrams = set(_bigram_tokens(q_tokens))
    d_bigrams = set(_bigram_tokens(d_tokens))
    bigram_overlap = 0.0
    if q_bigrams:
        bigram_overlap = float(len(q_bigrams & d_bigrams)) / float(len(q_bigrams))

    query_coverage = float(len(set(query_terms) & set(d_tokens))) / float(len(set(query_terms)) or 1)
    return min(1.0, (0.45 * phrase_boost) + (0.35 * bigram_overlap) + (0.20 * query_coverage))


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _recent_score(updated_at: Any) -> float:
    ts = _parse_datetime(updated_at)
    if ts is None:
        return 0.2
    delta_days = max(0, (utcnow() - ts.replace(tzinfo=None)).days)
    if delta_days <= 30:
        return 1.0
    if delta_days <= 90:
        return 0.7
    if delta_days <= 180:
        return 0.45
    if delta_days <= 365:
        return 0.25
    return 0.1


def upsert_knowledge_document(
    conn: Connection,
    *,
    source: str,
    title: str,
    area: str,
    content: str,
    tags: Optional[List[str]] = None,
) -> Dict[str, Any]:
    schema = schema_name()
    content = (content or "").strip()
    if not content:
        raise ValueError("content requerido")

    emb = build_embedding(content)
    emb_json = json.dumps(emb, ensure_ascii=False)
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

    row = conn.execute(
        text(
            f"SELECT id FROM {schema}.knowledge_documents "
            "WHERE content_hash = :h LIMIT 1"
        ),
        {"h": content_hash},
    ).mappings().first()

    payload = {
        "source": str(source or "N/A"),
        "title": str(title or "Documento"),
        "area": str(area or "GENERAL").upper(),
        "content": content,
        "tags_json": json.dumps(tags or [], ensure_ascii=False),
        "content_hash": content_hash,
        "embedding_model": "sentence-transformers-or-fallback",
        "embedding_dim": len(emb),
        "embedding_json": emb_json,
        "updated_at": utcnow(),
    }

    if row:
        doc_id = int(row["id"])
        conn.execute(
            text(
                f"""
                UPDATE {schema}.knowledge_documents
                SET source=:source,title=:title,area=:area,content=:content,tags_json=:tags_json,
                    embedding_model=:embedding_model,embedding_dim=:embedding_dim,
                    embedding_json=:embedding_json,active=TRUE,updated_at=:updated_at
                WHERE id=:id
                """
            ),
            {**payload, "id": doc_id},
        )
    else:
        doc_id = int(
            conn.execute(
                text(
                    f"""
                    INSERT INTO {schema}.knowledge_documents (
                        source,title,area,content,tags_json,content_hash,
                        embedding_model,embedding_dim,embedding_json,active,updated_at
                    ) VALUES (
                        :source,:title,:area,:content,:tags_json,:content_hash,
                        :embedding_model,:embedding_dim,:embedding_json,TRUE,:updated_at
                    ) RETURNING id
                    """
                ),
                payload,
            ).scalar_one()
        )

    if _is_postgres(conn):
        try:
            conn.execute(
                text(
                    f"UPDATE {schema}.knowledge_documents "
                    "SET embedding_vector = CAST(:v AS vector) WHERE id = :id"
                ),
                {"v": _vec_literal(emb), "id": doc_id},
            )
        except Exception:
            pass

    return {"id": doc_id, "title": payload["title"], "area": payload["area"]}


def upsert_knowledge_document_chunked(
    conn: Connection,
    *,
    source: str,
    title: str,
    area: str,
    content: str,
    tags: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    chunks = _chunk_content(content, max_chars=_CHUNK_SIZE, overlap_chars=_CHUNK_OVERLAP)
    if not chunks:
        return []
    if len(chunks) == 1:
        return [
            upsert_knowledge_document(
                conn,
                source=source,
                title=title,
                area=area,
                content=chunks[0],
                tags=tags,
            )
        ]

    base_tags = list(tags or [])
    document_key = hashlib.sha256(f"{source}|{title}|{area}".encode("utf-8")).hexdigest()[:20]
    created: List[Dict[str, Any]] = []
    total = len(chunks)
    for idx, chunk in enumerate(chunks, start=1):
        chunk_tags = list(base_tags)
        chunk_tags.extend([f"doc_key:{document_key}", f"chunk:{idx}/{total}"])
        created.append(
            upsert_knowledge_document(
                conn,
                source=source,
                title=f"{title} [chunk {idx}/{total}]",
                area=area,
                content=chunk,
                tags=chunk_tags,
            )
        )
    return created


def search_knowledge(conn: Connection, query: str, area: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
    schema = schema_name()
    q = (query or "").strip()
    if not q:
        return []
    lim = max(1, min(int(limit or 10), 100))
    candidate_limit = min(_MAX_CANDIDATES, max(30, lim * _CANDIDATE_FACTOR))
    query_terms = _expanded_query_terms(q)
    query_for_embedding = " ".join(query_terms) if query_terms else q

    emb = build_embedding(query_for_embedding)
    params: Dict[str, Any] = {"lim": candidate_limit, "v": _vec_literal(emb)}
    where = "WHERE active = TRUE"
    if area:
        where += " AND area = :area"
        params["area"] = str(area).upper()

    candidates: Dict[int, Dict[str, Any]] = {}

    # 1) candidatos vectoriales SQL (rápido con pgvector)
    if _is_postgres(conn):
        try:
            rows = conn.execute(
                text(
                    f"""
                    SELECT id, source, title, area, content,
                           LEFT(content, 420) AS preview, embedding_json, tags_json,
                           (1 - (embedding_vector <=> CAST(:v AS vector))) AS score,
                           updated_at
                    FROM {schema}.knowledge_documents
                    {where}
                    AND embedding_vector IS NOT NULL
                    ORDER BY embedding_vector <=> CAST(:v AS vector)
                    LIMIT :lim
                    """
                ),
                params,
            ).mappings().all()
            for r in rows:
                rid = int(r["id"])
                candidates[rid] = dict(r)
                candidates[rid]["_score_vector_sql"] = float(r["score"] or 0.0)
        except Exception:
            pass

    # 2) candidatos lexicales SQL para robustecer recall.
    lexical_terms = [t for t in query_terms if len(t) >= 3][:8]
    if lexical_terms:
        lex_params: Dict[str, Any] = {"lim": candidate_limit}
        lex_where = "WHERE active = TRUE"
        if area:
            lex_where += " AND area = :area"
            lex_params["area"] = str(area).upper()
        term_clauses = []
        for idx, token in enumerate(lexical_terms):
            key = f"t{idx}"
            term_clauses.append(f"LOWER(title || ' ' || content) LIKE :{key}")
            lex_params[key] = f"%{token}%"
        if term_clauses:
            lex_where += " AND (" + " OR ".join(term_clauses) + ")"
        preview_expr = "LEFT(content, 420)"
        if _is_sqlite(conn):
            preview_expr = "SUBSTR(content, 1, 420)"
        try:
            lex_rows = conn.execute(
                text(
                    f"""
                    SELECT id, source, title, area, content, {preview_expr} AS preview, embedding_json, tags_json, updated_at
                    FROM {schema}.knowledge_documents
                    {lex_where}
                    ORDER BY updated_at DESC
                    LIMIT :lim
                    """
                ),
                lex_params,
            ).mappings().all()
            for r in lex_rows:
                rid = int(r["id"])
                if rid not in candidates:
                    candidates[rid] = dict(r)
        except Exception:
            pass

    # 3) fallback base (latest docs) si aún no hay candidatos.
    preview_expr = "LEFT(content, 420)"
    if _is_sqlite(conn):
        preview_expr = "SUBSTR(content, 1, 420)"
    if not candidates:
        rows = conn.execute(
            text(
                f"""
                SELECT id, source, title, area, content, {preview_expr} AS preview, embedding_json, tags_json, updated_at
                FROM {schema}.knowledge_documents
                {where}
                ORDER BY updated_at DESC
                LIMIT :lim
                """
            ),
            {k: v for k, v in params.items() if k in {"lim", "area"}},
        ).mappings().all()
        for r in rows:
            rid = int(r["id"])
            candidates[rid] = dict(r)

    # 4) scoring híbrido + consolidación por documento base.
    scored_rows: List[Dict[str, Any]] = []
    for r in candidates.values():
        title = str(r.get("title") or "")
        content = str(r.get("content") or "")
        emb_doc: List[float] = []
        try:
            emb_doc = json.loads(r.get("embedding_json") or "[]")
        except Exception:
            emb_doc = []

        score_vec = float(r.get("_score_vector_sql") or 0.0)
        if score_vec <= 0.0:
            score_vec = _cosine(emb, emb_doc)

        tags: List[str] = []
        try:
            tags = json.loads(r.get("tags_json") or "[]")
            if not isinstance(tags, list):
                tags = []
        except Exception:
            tags = []

        score_lex = _lexical_score(query_terms, title, content, tags=tags)
        score_rerank = _rerank_score(q, query_terms, title, content)
        score_recent = _recent_score(r.get("updated_at"))
        score = (0.55 * score_vec) + (0.25 * score_lex) + (0.15 * score_rerank) + (0.05 * score_recent)

        scored_rows.append(
            {
                "id": int(r["id"]),
                "source": r.get("source"),
                "title": title,
                "base_title": _base_title(title),
                "area": r.get("area"),
                "preview": r.get("preview"),
                "score": round(float(score), 6),
                "score_vector": round(float(score_vec), 6),
                "score_lexical": round(float(score_lex), 6),
                "score_rerank": round(float(score_rerank), 6),
                "updated_at": r["updated_at"].isoformat() if hasattr(r.get("updated_at"), "isoformat") else (str(r.get("updated_at")) if r.get("updated_at") else None),
            }
        )

    scored_rows.sort(key=lambda x: x["score"], reverse=True)

    # Consolidar chunks para devolver un solo resultado por documento base.
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in scored_rows:
        group_key = f"{row.get('source')}::{row.get('area')}::{row.get('base_title')}"
        existing = grouped.get(group_key)
        if existing is None:
            row["matched_chunks"] = 1
            row.pop("base_title", None)
            grouped[group_key] = row
        else:
            existing["matched_chunks"] = int(existing.get("matched_chunks") or 1) + 1
            if float(row.get("score") or 0.0) > float(existing.get("score") or 0.0):
                existing.update(
                    {
                        "id": row["id"],
                        "title": row["title"],
                        "preview": row["preview"],
                        "score": row["score"],
                        "score_vector": row["score_vector"],
                        "score_lexical": row["score_lexical"],
                        "score_rerank": row["score_rerank"],
                        "updated_at": row["updated_at"],
                    }
                )

    out = list(grouped.values())
    out.sort(key=lambda x: (float(x.get("score") or 0.0), float(x.get("score_vector") or 0.0)), reverse=True)
    return out[:lim]


def load_default_corpus(conn: Connection) -> Dict[str, Any]:
    seed_path = Path(__file__).resolve().parent / "knowledge" / "corpus_seed.json"
    if not seed_path.exists():
        return {"loaded": 0, "error": f"seed not found: {seed_path}"}

    data = json.loads(seed_path.read_text(encoding="utf-8"))
    loaded = 0
    documents = 0
    for d in data:
        docs = upsert_knowledge_document_chunked(
            conn,
            source=str(d.get("source") or "N/A"),
            title=str(d.get("title") or "Documento"),
            area=str(d.get("area") or "UROLOGIA"),
            content=str(d.get("content") or ""),
            tags=list(d.get("tags") or []),
        )
        loaded += len(docs)
        documents += 1
    return {
        "loaded": loaded,
        "documents": documents,
        "source": str(seed_path),
        "chunk_size": _CHUNK_SIZE,
        "chunk_overlap": _CHUNK_OVERLAP,
    }

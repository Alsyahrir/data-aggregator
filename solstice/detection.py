import json
import re
import warnings
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from .schema import SCHEMA


# Keywords shorter than this are too generic to trust as a bare substring
# ("id" is inside "humidity", "pf" is inside "pf_ratio"), so they only ever
# match on a word boundary.
_MIN_SUBSTRING_KEYWORD = 3

# How many values to sample when checking whether a column really holds dates.
_TIMESTAMP_SAMPLE = 200

# Big enough to outrank any name-based difference: a column proven to hold
# real dates beats one that merely has a better-looking name.
_DATE_SPAN_BONUS = 500


def _normalise(text: str) -> str:
    """Lowercase and turn separators into spaces so word-boundary matching
    works: 'Measured_On' -> 'measured on', 'AC-Output(kWh)' -> 'ac output kwh'.
    """
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _veto(field_name: str, column_lower: str) -> bool:
    """Domain rules for columns that superficially look like a field but
    aren't. Returns True if this column must not map to this field.
    """
    if field_name == "energy":
        # Voltage / power-factor columns often contain energy-ish words.
        return any(x in column_lower for x in ("voltage", "power_factor", "pf"))
    if field_name == "ambient_temp":
        # Only genuine ambient readings, not module/cell temperatures.
        return "ambient" not in column_lower
    if field_name == "irradiance":
        # The schema field is GHI; DNI is a different measurement.
        return "dni" in column_lower
    return False


def _score(column: str, field_name: str, keywords: List[str]) -> int:
    """How strongly `column` looks like `field_name`. 0 means no match.

    Tiers, strongest first:
      exact     the whole column name IS the keyword        ('ghi' -> ghi)
      boundary  the keyword appears as a whole word         ('energy_kwh' -> energy)
      substring the keyword appears anywhere                ('energykwh' -> energy)

    Longer keywords win inside a tier, so 'ambient_temperature' beats a bare
    'temp'-style match. Scoring (rather than the old first-match-wins) is what
    stops a column's position in the file from deciding the mapping.
    """
    col_norm = _normalise(column)
    col_lower = column.lower()
    best = 0

    for keyword in keywords:
        kw_norm = _normalise(keyword)
        if not kw_norm:
            continue

        if col_norm == kw_norm:
            best = max(best, 1000 + len(kw_norm))
        elif re.search(rf"{re.escape(kw_norm)}", col_norm):
            best = max(best, 100 + len(kw_norm))
        elif len(kw_norm) >= _MIN_SUBSTRING_KEYWORD and keyword.lower() in col_lower:
            best = max(best, 10 + len(kw_norm))

    return best


def _timestamp_bonus(series: pd.Series) -> int:
    """Reward a timestamp candidate whose values actually span several
    calendar dates.

    Names alone are not enough here. Real exports ship a `Date` column
    holding the date next to a `Timestamp` column holding only a constant
    time-of-day; the better-looking name is the useless one. Parsing the
    values is the only way to tell them apart.
    """
    sample = series.dropna().head(_TIMESTAMP_SAMPLE)
    if len(sample) < 2:
        return 0

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            parsed = pd.to_datetime(sample, errors="coerce", format="mixed")
    except (ValueError, TypeError):
        return 0

    parsed = parsed.dropna()
    if len(parsed) < 2 or len(parsed) / len(sample) < 0.5:
        return 0

    return _DATE_SPAN_BONUS if parsed.dt.date.nunique() > 1 else 0


def auto_detect_columns(df: pd.DataFrame) -> Tuple[Dict[str, str], str]:
    """Automatically detect column mappings using keywords.

    Every (column, field) pair is scored, then the strongest pairs are
    assigned first — so the best candidate for a field wins regardless of
    where it sits in the file, and a weak match can no longer claim a slot
    and leave the real column unmapped.
    """
    candidates = []  # (score, column_index, column, field_name)

    for col_index, column in enumerate(df.columns):
        column_lower = str(column).lower()
        for field_name, field_def in SCHEMA.items():
            if _veto(field_name, column_lower):
                continue
            score = _score(str(column), field_name, field_def.keywords)
            if score > 0:
                if field_name == "timestamp":
                    score += _timestamp_bonus(df[column])
                candidates.append((score, col_index, str(column), field_name))

    # Strongest first; ties fall back to column order for stable output.
    candidates.sort(key=lambda c: (-c[0], c[1]))

    mapping: Dict[str, str] = {}
    taken_fields = set()
    field_column_index: Dict[str, int] = {}

    for score, col_index, column, field_name in candidates:
        if column in mapping or field_name in taken_fields:
            continue
        mapping[column] = field_name
        taken_fields.add(field_name)
        field_column_index[field_name] = col_index

    # Restore original column order in the returned mapping.
    mapping = {
        col: mapping[str(col)]
        for col in df.columns
        if str(col) in mapping
    }

    return mapping, _infer_file_type(taken_fields, field_column_index)


def _infer_file_type(taken_fields: set, field_column_index: Dict[str, int]) -> str:
    """Classify the file from the fields that were matched. Energy always
    wins (it makes the file an inverter export); otherwise the earliest of
    ambient_temp / irradiance decides, matching the original behaviour.
    """
    if "energy" in taken_fields:
        return "inverter"

    type_by_field = {"ambient_temp": "environment", "irradiance": "irradiance"}
    present = [f for f in type_by_field if f in taken_fields]
    if not present:
        return "unknown"

    earliest = min(present, key=lambda f: field_column_index[f])
    return type_by_field[earliest]


def generate_llm_prompt(df: pd.DataFrame, filename: str = "data.csv") -> str:
    """Generate prompt for LLM to suggest mappings."""
    columns_info = []
    for col in df.columns:
        samples = df[col].dropna().head(3).tolist()
        columns_info.append({
            "column_name": col,
            "data_type": str(df[col].dtype),
            "sample_values": [str(v) for v in samples],
        })
    
    return f"""Map columns from "{filename}" to schema:
- timestamp, energy, ambient_temp, irradiance, wind_speed, module_temp, humidity

COLUMNS:
{json.dumps(columns_info, indent=2)}

Respond with JSON: {{"mapping": {{"source": "target"}}, "file_type": "inverter|environment"}}
"""


def parse_llm_response(response: str) -> Dict[str, Any]:
    """Parse LLM JSON response."""
    text = response.strip()
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]
    
    start = text.find("{")
    end = text.rfind("}") + 1
    if start == -1:
        raise ValueError("No JSON found")
    
    result = json.loads(text[start:end])
    result.setdefault("mapping", {})
    result.setdefault("file_type", "unknown")
    return result


def format_llm_result_for_review(result: Dict[str, Any]) -> str:
    """Format LLM result for review."""
    lines = ["=" * 60, "LLM MAPPING", "=" * 60, f"Type: {result.get('file_type')}", "Mappings:"]
    for src, tgt in result.get("mapping", {}).items():
        lines.append(f"  '{src}' -> '{tgt}'")
    return "\n".join(lines)

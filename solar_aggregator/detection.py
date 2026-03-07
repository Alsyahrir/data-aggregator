"""
Part 2: Column Detection
"""

import json
import pandas as pd
from typing import Dict, List, Tuple, Any
from .schema import SCHEMA


def auto_detect_columns(df: pd.DataFrame) -> Tuple[Dict[str, str], str]:
    """Automatically detect column mappings using keywords."""
    mapping = {}
    file_type = "unknown"
    
    for column in df.columns:
        column_lower = column.lower()
        
        for field_name, field_def in SCHEMA.items():
            if field_name in mapping.values():
                continue
            
            for keyword in field_def.keywords:
                if keyword in column_lower:
                    if field_name == "energy":
                        if any(x in column_lower for x in ['voltage', 'power_factor', 'pf']):
                            continue
                        mapping[column] = field_name
                        file_type = "inverter"
                        break
                    elif field_name == "ambient_temp":
                        if 'ambient' in column_lower:
                            mapping[column] = field_name
                            if file_type == "unknown":
                                file_type = "environment"
                            break
                    elif field_name == "irradiance":
                        if 'dni' in column_lower:
                            continue
                        mapping[column] = field_name
                        if file_type == "unknown":
                            file_type = "irradiance"
                        break
                    else:
                        mapping[column] = field_name
                        break
    
    return mapping, file_type


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

from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import airportsdata


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / 'app' / 'lib' / 'generated-airport-supplement.ts'


def normalize(value: str) -> str:
    normalized = unicodedata.normalize('NFD', value or '')
    return ''.join(ch for ch in normalized if unicodedata.category(ch) != 'Mn').lower().strip()


def build_aliases(code: str, name: str, city: str) -> list[str]:
    normalized_code = normalize(code)
    normalized_name = normalize(name)
    normalized_city = normalize(city)

    aliases: list[str] = []
    for alias in (
        normalized_code,
        f'{normalized_code} {normalized_city}' if normalized_city else '',
        normalized_city,
        f'{normalized_city} {normalized_code}' if normalized_city else '',
        normalized_name,
    ):
        if alias and alias not in aliases:
            aliases.append(alias)

    return aliases


def main() -> None:
    airport_data = airportsdata.load('IATA')
    entries = []

    for code, airport in sorted(airport_data.items()):
        if not code or len(code) != 3 or not code.isalpha():
            continue

        name = (airport.get('name') or '').strip()
        city = (airport.get('city') or '').strip()
        country = (airport.get('country') or '').strip()
        if not name or not country:
            continue

        entries.append(
            {
                'code': code,
                'name': name,
                'type': 'airport',
                'country': country,
                'city': city or None,
                'aliases': build_aliases(code, name, city),
            }
        )

    body = json.dumps(entries, indent=2, ensure_ascii=False)
    body = body.replace(': null', ': undefined')

    OUTPUT_PATH.write_text(
        "// Generated from airportsdata.\n"
        "// Do not edit by hand.\n\n"
        "import type { GeneratedLocationEntry } from './generated-locations'\n\n"
        "export const GENERATED_AIRPORT_SUPPLEMENT: GeneratedLocationEntry[] = "
        f"{body}\n",
        encoding='utf-8',
    )


if __name__ == '__main__':
    main()
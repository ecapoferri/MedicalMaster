import re
from pathlib import Path
"""
Uses the drop-down list element ('<select>') html
    from html source of the email form page to create a
    newline delimited list to use in the AFDir.xlsx .
"""
SRC = 'lead_email_form-practice_options.html'
OUT = 'lead_email_form-practice_option-cleanup.txt'

REPL_PTNS = [
    r'<select.*',
    r' {4}',
    r'\n</select>',
    r'<option value="',
    r'">.*',
    r'</option>',
]
RE_PTN = r'|'.join(REPL_PTNS)

def clean(in_path: str) -> str:
    src_file = Path(in_path)
    in_string = src_file.read_text()

    out_str = (
        re.sub('\n\n', '\n',
            re.sub(RE_PTN, '', in_string)
        )
        .strip()
    )

    return out_str


def main() -> None:
    out_file = Path(OUT)
    out_file.write_text(clean(SRC))
    return


if __name__ == "__main__":
    main()

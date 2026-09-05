from __future__ import annotations

from stream_mining_labs_python.countminsketch.cms_row import CMSRow


class CMS:
    def __init__(self, row_count: int = 5, column_count: int = 12) -> None:
        self.row_count = row_count
        self.column_count = column_count
        self.cms_table = [CMSRow(column_count) for _ in range(row_count)]

    def process_element(self, value: str) -> int:
        for row in self.cms_table:
            row.hash_value(value)
        return min(row.query(value) for row in self.cms_table)

# Data layout (SoionOption)

SoionOption is a *consumer* of option_chain parquet snapshots (it does not own ingestion).

Folders:
- data/raw/option_chain/...      raw dumps (optional)
- data/cleaned/option_chain/...  normalized/cleaned snapshots
- data/sample/option_chain/...   small sample used for tests / demos

Do not commit large data. Keep sample tiny.

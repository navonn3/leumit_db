# 🏀 Leumit League Database

Auto-updated Israeli Basketball Leumit League statistics database.

## 📊 Available Data

All CSV files are updated daily at 2:00 AM Israel time.

### Player Data
- **Player Details**: `data/leumit/leumit_player_details.csv`
- **Player History**: `data/leumit/leumit_player_history.csv`
- **Player Averages**: `data/leumit/leumit_player_averages.csv`

### Team Data
- **Team Averages**: `data/leumit/leumit_team_averages.csv`
- **Opponent Averages**: `data/leumit/leumit_opponent_averages.csv`

### Game Data
- **Games Schedule**: `data/leumit/leumit_games/games_schedule.csv`
- **Game Quarters**: `data/leumit/leumit_games/game_quarters.csv`
- **Game Player Stats**: `data/leumit/leumit_games/game_player_stats.csv`
- **Game Team Stats**: `data/leumit/leumit_games/game_team_stats.csv`

## 🔗 Direct CSV Links

Use these URLs to access the data directly:

```
https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_player_details.csv
https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_player_averages.csv
https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_team_averages.csv
https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_opponent_averages.csv
https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_games/games_schedule.csv
https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_games/game_quarters.csv
https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_games/game_player_stats.csv
https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_games/game_team_stats.csv
```

## 🔄 Update Schedule

- **Automatic**: Daily at 2:00 AM Israel time
- **Manual**: Can be triggered from GitHub Actions tab

## 📝 Data Schema

See full schema documentation in the repository.

### Team Name Normalization

All team names are normalized using `data/leumit/team_names.csv` to ensure consistency across all data files.

## 🚀 Usage Examples

### Python
```python
import pandas as pd

# Load player averages
url = "https://raw.githubusercontent.com/navonn3/leumit_db/main/data/leumit/leumit_player_averages.csv"
df = pd.read_csv(url, encoding='utf-8-sig')

# Top 10 scorers
top_scorers = df.nlargest(10, 'pts')
print(top_scorers[['player_name', 'team', 'pts', 'games_played']])
```

### Excel / Google Sheets
1. Data → Get Data → From Web
2. Paste any CSV URL from above
3. Data will refresh when you reload

### BASE44
1. Create new data source
2. Use CSV URL as source
3. Set refresh schedule

## 📊 Last Update

Check the commit history to see when data was last updated.

## ⚙️ Technical Details

- **Source**: [ibasketball.co.il](https://ibasketball.co.il)
- **Update Script**: `auto-update-db.py`
- **Automation**: GitHub Actions
- **Encoding**: UTF-8-BOM
- **Team Normalization**: Automatic via mapping file

## 📞 Issues

If you notice any data inconsistencies, please open an issue.

---

**Status**: 🟢 Active and updating daily

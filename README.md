# Code Smell Detection

This project collects Python functions from GitHub and automatically labels 4 code smells:

- Long Function
- Too Many Parameters
- High Cyclomatic Complexity
- Deep Nesting

## Setup

1. Create venv `python3 -m venv .venv`
2. Activate it and install `pip install -r requirements.txt`
3. Add your GitHub token in .env (see .env.example)

## Next step

Run the repo collection script in scripts/collect_repos.py

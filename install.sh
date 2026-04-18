#!/usr/bin/env bash
set -e
python3 -m venv venv
source venv/bin/activate
pip install playwright beautifulsoup4 pandas requests pytest aiohttp lxml
playwright install chromium
cat << 'EOF' > sss
#!/usr/bin/env bash
source "$(dirname "$0")/venv/bin/activate"
python "$(dirname "$0")/scraper.py" "$@"
EOF
chmod +x sss
if [[ ":$PATH:" != *":$(pwd):"* ]]; then
    echo "export PATH=\"$(pwd):\$PATH\"" >> ~/.zshrc
    echo "export PATH=\"$(pwd):\$PATH\"" >> ~/.bashrc
fi
echo "Installation complete. Please run 'source ~/.zshrc' or open a new terminal, then type 'sss' to begin."
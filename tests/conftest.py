"""
Fixtures compartilhadas entre os módulos de teste.
"""
import sys
from pathlib import Path

# Garante que o pacote raiz está no path mesmo sem pip install -e .
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

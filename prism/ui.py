"""
UI for logging events

Table of Contents
- ANSI color codes
"""

####################
# ANSI color codes #
####################

BLACK = "\u001b[30m"
RED = "\u001b[31m"
GREEN = "\u001b[32m"
YELLOW = "\u001b[33m"
BLUE = "\u001b[38;5;69m"
PURPLE = "\u001b[38;5;99m"
MAGENTA = "\u001b[38;5;170m"
CYAN = "\u001b[36m"
WHITE = "\u001b[37m"
RESET = "\u001b[0m"
BRIGHT_WHITE = "\u001b[37;1m"
BRIGHT_YELLOW = "\u001b[33;1m"
BRIGHT_GREEN = "\u001b[32;1m"
BOLD = "\u001b[1m"
HEADER_GRAY = "\u001b[0m"
GRAY_PINK = "\u001b[38;5;96m"
ORANGE_BROWN = "\u001b[38;5;180m"

# Event colors
EVENT_COLOR = "\u001b[38;5;103m"

# Agent colors
AGENT_EVENT = "\u001b[38;5;32m"
AGENT_WHICH_BUILD = "\u001b[38;5;178m"
AGENT_WHICH_RUN = "\u001b[38;5;10m"


##################
# Terminal width #
##################

TERMINAL_WIDTH = 80

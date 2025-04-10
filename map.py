import streamlit as st
import os
import datetime

# Import our visualization modules
from carpathian_bears import display_carpathian_bears_section
from japan_deterrents import JapanDeterrentSystem

# Set page configuration
st.set_page_config(page_title="Wildlife Movement Analysis System", layout="wide")

# Create the main navigation tabs
main_tab1, main_tab2 = st.tabs(["Bear Deterrent System (Japan)", "Carpathian Bears Movement"])

# Display the Japan Deterrent System tab
with main_tab1:
    # Initialize the Japan Deterrent System object
    japan_system = JapanDeterrentSystem()
    # Display the Japan Deterrent System
    japan_system.display_japan_deterrent_section()

# Display the Carpathian Bears tab
with main_tab2:
    # Call the Carpathian bears visualization function
    display_carpathian_bears_section()

# Add a footer
st.markdown("""
---
### About this Application

This integrated system provides tools for:
1. **Japan Bear Deterrent System** - Track and manage bear deterrent devices in Japan
2. **Carpathian Bears Movement** - Analyze movement patterns of brown bears in the Carpathian Mountains

Developed for wildlife research and management.
""")
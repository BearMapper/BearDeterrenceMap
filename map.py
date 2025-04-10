import streamlit as st
import os
import time

# Set page configuration
st.set_page_config(page_title="Wildlife Movement Analysis System", layout="wide")

# Store which tab is active in session state
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "japan"  # Default to Japan tab

# Create tab selection with radio buttons
st.sidebar.title("Select System")
selected_tab = st.sidebar.radio(
    "Choose Visualization System:",
    ["Japan Bear Deterrent System", "Carpathian Bears Movement"],
    index=0 if st.session_state.active_tab == "japan" else 1,
    key="tab_selector"
)

# Update the active tab based on selection
if selected_tab == "Japan Bear Deterrent System":
    st.session_state.active_tab = "japan"
else:
    st.session_state.active_tab = "carpathian"

# Clear the rest of the sidebar for the component's filters
st.sidebar.markdown("---")

# Main content area
st.title(selected_tab)

# Only import and run the active component
if st.session_state.active_tab == "japan":
    # Only import Japan module when needed
    try:
        from japan_deterrents import JapanDeterrentSystem
        # Initialize and display Japan system
        japan_system = JapanDeterrentSystem()
        japan_system.display_japan_deterrent_section()
    except Exception as e:
        st.error(f"Error loading Japan Deterrent System: {str(e)}")
        st.info("Please check that the japan_deterrents.py module is properly installed.")
else:  # Carpathian tab is active
    # Add loading indicator for Carpathian system
    with st.spinner("Loading Carpathian Bears Visualization..."):
        try:
            # Display loading progress to manage user expectations
            progress_placeholder = st.empty()
            progress_bar = progress_placeholder.progress(0)
            progress_text = st.empty()
            
            # First stage - module import
            progress_text.text("Importing modules...")
            for i in range(20):
                time.sleep(0.02)
                progress_bar.progress(i)
            
            # Only import Carpathian module when needed - this is the key separation
            from carpathian_bears import display_carpathian_bears_section
            
            # Second stage - data loading preparation
            progress_text.text("Preparing data structures...")
            for i in range(20, 40):
                time.sleep(0.02)
                progress_bar.progress(i)
                
            # Final stage - handing over to the component
            progress_text.text("Initializing visualization...")
            for i in range(40, 100):
                time.sleep(0.01)
                progress_bar.progress(i)
                
            # Clear the progress indicators
            progress_placeholder.empty()
            progress_text.empty()
            
            # Display the Carpathian visualization
            display_carpathian_bears_section()
            
        except Exception as e:
            st.error(f"Error loading Carpathian Bears Visualization: {str(e)}")
            st.info("Please check that the carpathian_bears.py module is properly installed.")

# Add a footer
st.markdown("""
---
### About this Application

This integrated system provides tools for:
1. **Japan Bear Deterrent System** - Track and manage bear deterrent devices in Japan
2. **Carpathian Bears Movement** - Analyze movement patterns of brown bears in the Carpathian Mountains

Developed for wildlife research and management.
""")
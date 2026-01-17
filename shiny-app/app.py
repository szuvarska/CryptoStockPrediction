from shiny import App
from ui_layout import app_ui
from server_logic import server

# Static Assets Directory
www_dir = "/opt/shiny-app/www"

# Create the Shiny App
app = App(app_ui, server, static_assets=www_dir)
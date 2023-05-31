# external dependencies
import plotly.express as px
from dash import Dash
import dash_bootstrap_components as dbc

# local dependencies
import UI

# Build App UI
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = UI.default_UI(app)

# Run app and display result inline in the notebook
if __name__ == "__main__":
    app.run_server(debug="True")
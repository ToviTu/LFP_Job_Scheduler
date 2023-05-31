# External Modules
from dash import Dash, html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import sys
import plotly

def default_UI(app: Dash) -> html:

    return html.Div(
        [
            dbc.NavbarSimple(
                children=[
                    dbc.NavItem(dbc.NavLink("Documentation", href="#")),
                    dbc.DropdownMenu(
                        children=[
                            dbc.DropdownMenuItem("More pages", header=True),
                            dbc.DropdownMenuItem("Page 2", href="#"),
                            dbc.DropdownMenuItem("Page 3", href="#"),
                        ],
                        nav=True,
                        in_navbar=True,
                        label="More",
                    ),
                ],
                brand="Dashboard",
                brand_href="#",
                color="primary",
                dark=True,
            )
        ]
    )
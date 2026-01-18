from shiny import ui
from faicons import icon_svg
from config import ALL_ASSETS, TIME_RANGE_CHOICES

app_ui = ui.page_sidebar(
    # --- GLOBAL SIDEBAR ---
    ui.sidebar(
        ui.panel_conditional(
            "input.tabs != 'Monitoring'", # && input.tabs != 'Testy Michała'",
            ui.h4("Filters", class_="sidebar-title"),
            ui.input_select(
                "crypto_select", "Asset:",
                ALL_ASSETS,
                selected="BTC"
            ),
            ui.input_select(
                "time_range", "Time Range:",
                TIME_RANGE_CHOICES,
                selected="24H"
            ),
        ),
        ui.hr(),
        ui.h6("System Health"),
        ui.output_ui("sidebar_status"),
        width=275,
        open="desktop"
    ),

    ui.head_content(
        ui.tags.link(rel="stylesheet", href="styles.css"),
        ui.tags.link(rel="icon", href="https://cdn-icons-png.flaticon.com/512/1822/1822219.png")
    ),

    ui.navset_card_tab(
        # --- TAB 1: DASHBOARD ---
        ui.nav_panel("Dashboard",
                     ui.h3("Live Market Overview", class_="tab-header"),
                     # Value Boxes (Metrics)
                     ui.layout_columns(
                         ui.value_box(
                             "Current Price", ui.output_ui("vbox_price"),
                             showcase=icon_svg("bitcoin"), theme="bg-gradient-blue-purple"
                         ),
                         ui.value_box(
                             ui.output_ui("vbox_change_label"), ui.output_ui("vbox_change"),
                             showcase=icon_svg("chart-line"), theme="white"
                         ),
                         ui.value_box(
                             "Volatility (StdDev)", ui.output_ui("vbox_vol"),
                             showcase=icon_svg("wave-square"), theme="bg-gradient-blue-purple"
                         ),
                         fill=False
                     ),
                     ui.br(),
                     ui.card(
                         ui.card_header("Price Trend Analysis"),
                         ui.output_ui("price_chart_view"),
                         full_screen=True
                     ),
                     ui.br(),
                     ui.layout_columns(
                         ui.card(
                             ui.card_header(
                                 ui.div(
                                     ui.span("Candlestick Analysis"),
                                     ui.input_switch("show_sma", "Show SMAs", True),
                                     class_="d-flex justify-content-between align-items-center"
                                 )
                             ),
                             ui.output_ui("candle_chart_view"),
                             full_screen=True
                         ),
                         col_widths=[12]
                     )
                     ),

        # --- TAB 2: EDA (Integrated) ---
        ui.nav_panel("EDA",
                     ui.h3("Exploratory Data Analysis", class_="tab-header"),
                     ui.row(
                         ui.column(6, ui.card(ui.output_ui("corr_chart_view"))),
                         ui.column(6, ui.card(ui.output_ui("dist_chart_view")))
                     ),
                     ui.row(
                         ui.column(6, ui.card(ui.output_ui("stock_chart_view"))),
                         ui.column(6, ui.card(ui.output_ui("forex_chart_view")))
                     )
                     # )
                     ),

        # --- TAB 3: MODEL EVALUATION ---
        ui.nav_panel("Model Eval",
                     ui.h3("Model Scorecard", class_="tab-header"),
                     ui.layout_columns(
                         ui.value_box("RMSE", ui.output_ui("eval_rmse"), theme="danger"),
                         ui.value_box("MAPE", ui.output_ui("eval_mape"), theme="warning"),
                         ui.value_box("Directional Acc", ui.output_ui("eval_dir"), theme="success"),
                         ui.value_box("R-Squared", ui.output_ui("eval_r2"), theme="primary"),
                         fill=False
                     ),
                     ui.br(),
                     ui.row(
                         ui.column(6, ui.card(ui.card_header("Actual vs Predicted"), ui.output_ui("eval_pred_chart"))),
                         ui.column(6, ui.card(ui.card_header("Residuals"), ui.output_ui("eval_resid_chart")))
                     )
                     ),

        # --- TAB 4: MONITORING ---
        ui.nav_panel("Monitoring",
                     ui.h3("Pipeline Health Monitor", class_="tab-header"),
                     ui.layout_columns(
                         ui.value_box("Pipeline Status", ui.output_ui("mon_status_main")),
                         ui.value_box("Data Freshness", ui.output_ui("mon_freshness")),
                         ui.value_box("Data Refresh", ui.output_ui("mon_latency")),
                         fill=False
                     ),
                     ui.br(),
                     ui.card(
                         ui.card_header("Data Ingestion Latency Log"),
                         ui.output_table("mon_table")
                     )
                     ),

        # --- TAB 5: RAW DATA ---
        ui.nav_panel("Raw Data",
                     ui.h3("Raw Data", class_="tab-header"),
                     ui.layout_columns(
                         ui.download_button("download_csv", "Download CSV", class_="btn-primary"),
                         ui.input_select(
                             "source_select", label="",
                             choices={"crypto": "Cryptocurrency Prices", "forex": "Forex Prices"},
                             selected="Crypto"
                         ),
                         col_widths=[3]
                     ),
                     ui.br(),
                     ui.card(ui.output_table("raw_table"))
                     ),
        id="tabs",
    ),
    ui.output_ui("dynamic_footer"),
    title=ui.span(
        ui.img(src="https://cdn-icons-png.flaticon.com/512/1822/1822219.png", class_="app-logo"),
        "CRYPTO ANALYTICS",
        class_="app-brand"
    )
)

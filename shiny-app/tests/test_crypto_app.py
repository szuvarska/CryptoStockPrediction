import pytest
import logging
from playwright.sync_api import Page, expect
import re

# Set up logging for the report
logging.basicConfig(
    filename='tests/pytest.log',  # Log output to this file
    level=logging.INFO,  # Set log level to INFO or DEBUG as needed
    format="%(asctime)s - %(levelname)s - %(message)s"  # Log format
)
logger = logging.getLogger(__name__)

# Use the internal loopback if testing inside the same container
BASE_URL = "http://127.0.0.1:8000"


@pytest.fixture(scope="module")
def shared_page(browser):
    """Reuse a single page and provide a massive timeout for backend data processing."""
    page = browser.new_page()
    # 10-minute timeout for the initial connection and page load
    page.set_default_navigation_timeout(600000)
    try:
        page.goto(BASE_URL, wait_until="load")
        logger.info("Successfully connected to the App.")
    except Exception as e:
        logger.error(f"App failed to load within timeout: {e}")
        raise e
    yield page
    page.close()


def test_container_health(shared_page: Page):
    """TC-101: Verify Branding and UI Load."""
    # Ensure the brand name exists in the UI header
    brand = shared_page.locator(".app-brand")
    expect(brand).to_contain_text("CRYPTO ANALYTICS", timeout=10000)
    logger.info("TC-101: Branding verified.")


def test_vbox_price_load(shared_page: Page):
    """TC-102: Validate HBase Data Pipeline flow."""
    logger.info("Monitoring #vbox_price. Waiting for scan (Est: 8 mins)...")
    vbox = shared_page.locator("#vbox_price")

    # FIX: Use re.compile for regex matching
    expect(vbox).to_have_text(re.compile(r"\$\d+"), timeout=600000)

    price_text = vbox.inner_text()
    logger.info(f"ACTUAL RESULT: Price successfully loaded: {price_text}")
    assert "$" in price_text


def test_spark_monitoring_status(shared_page: Page):
    """TC-103: Verify Pipeline Health via Spark."""
    shared_page.click("text=Monitoring")
    logger.info("Tab changed to Monitoring. Waiting for Spark-SQL table...")

    table = shared_page.locator("#mon_table")
    # FIX: to_have_count does not support Regex. Use a number >= 2 (Header + 1 row).
    # We wait for the table to populate
    expect(table).to_be_visible(timeout=120000)

    # Verify table has at least 2 rows (Header + at least one data row)
    count = table.locator("tr").count()
    assert count >= 2, f"Expected at least 2 rows in monitoring table, found {count}"
    logger.info(f"TC-203 Passed: Spark monitoring table has {count} rows.")


def test_hdfs_model_debug(shared_page: Page):
    """TC-104: Verify HDFS Model metadata."""
    shared_page.click("text=Testy Michała")
    model_output = shared_page.locator("pre")
    expect(model_output).to_contain_text("LinearRegressionModel", timeout=60000)
    logger.info("TC-205 Passed: Model loaded correctly from HDFS.")


def test_raw_data_download(shared_page: Page):
    """TC-105: Verify CSV Download functionality."""
    shared_page.click("text=Raw Data")

    with shared_page.expect_download() as download_info:
        shared_page.click("#download_csv")

    download = download_info.value

    assert download.suggested_filename == "crypto_data.csv"

    path = download.path()
    assert path.stat().st_size > 0, "Downloaded CSV is empty"

    logger.info(f"TC-301: Download successful. File: {download.suggested_filename}")


def test_filter_asset_change(shared_page: Page):
    """TC-106: Verify changing Asset filter updates the Value Box."""
    shared_page.click("text=Dashboard")

    shared_page.get_by_label("Asset:").select_option("ETH")

    vbox = shared_page.locator("#vbox_price")
    expect(vbox).not_to_have_class(re.compile(r"recalculating"), timeout=10000)

    expect(vbox).to_contain_text("$", timeout=5000)
    logger.info("TC-105: Asset filter switched to ETH successfully.")


def test_time_range_filter(shared_page: Page):
    """TC-107: Verify Time Range filter updates the graphs."""
    shared_page.click("text=Dashboard")

    price_chart = shared_page.locator("#price_chart_view")

    shared_page.get_by_label("Time Range:").select_option("7D")

    expect(price_chart).to_have_class(re.compile(r"recalculating"), timeout=5000)
    expect(price_chart).not_to_have_class(re.compile(r"recalculating"), timeout=30000)

    logger.info("TC-106: Time range changed to 7D and chart updated.")

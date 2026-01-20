import pytest
import base64
from datetime import datetime
from playwright.sync_api import Page


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # Get the HTML plugin
    pytest_html = item.config.pluginmanager.getplugin("html")

    # Execute the test and get the outcome
    outcome = yield
    report = outcome.get_result()

    # Get existing extras (or create list)
    extra = getattr(report, "extras", [])

    if report.when == "call":
        # Check if the test failed
        if report.failed:
            # Retrieve the 'shared_page' fixture if it exists in this test
            page = item.funcargs.get("shared_page")
            if page:
                # 1. Capture screenshot as raw bytes
                screenshot_bytes = page.screenshot()

                # 2. Encode to Base64 string (Fix for AttributeError)
                # content MUST be a string for pytest-html to embed it correctly
                image_b64 = base64.b64encode(screenshot_bytes).decode("utf-8")

                # 3. Add to report
                extra.append(pytest_html.extras.image(image_b64, mime_type="image/png"))

        report.extras = extra
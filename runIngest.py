"""
This code creates a native popup window that renders dashboard.html 
(HTML/CSS/JS) using the `pywebview` library.

Run with:
    python webview_dashboard.py

Requires dataIngest.py and dashboard.html in the same folder as this file.
"""
import json
import subprocess
import sys
from pathlib import Path

import webview

# These search for the needed files in the same folder as this script
SCRIPT_PATH = Path(__file__).parent / "dataIngest.py"
HTML_PATH = Path(__file__).parent / "dashboard.html"

"""
This class is the bridge between the Python backend and the
JavaScript frontend. Every public method on this class
is callable from JS as 'pywebview.api.<method_name>(...)'.
"""
class Api:
    def __init__(self):
        self.window = None
        self.process = None

    # Sets the window reference so we can call window.evaluate_js(...) later
    def set_window(self, window):
        self.window = window

    # Push a line of text into the page's log box by calling the
    # JavaScript function `appendLog` defined in dashboard.html.
    def _log(self, text: str):
        safe_text = json.dumps(text)
        self.window.evaluate_js(f"appendLog({safe_text})")

    # Exposed to JS as pywebview.api.run_ingestion(config)
    def run_ingestion(self, config: dict):
        # If a job is currently running, block the new one
        if self.process is not None:
            self._log("A job is already running — please wait for it to finish.\n")
            return

        # Builds the command line to run dataIngest.py with the given config
        command = self._build_command(config)
        self._log(f"Running: {' '.join(command)}\n\n")

        try:
            # Run the command in a subprocess, capturing stdout and stderr
            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            # Push each line of output into the log box in real time
            for line in self.process.stdout:
                self._log(line)

            # Wait for the process to finish and log the exit code
            self.process.wait()
            self._log(f"\nFinished with exit code {self.process.returncode}.\n")
        except FileNotFoundError:
            self._log(f"\nCould not find {SCRIPT_PATH}. Make sure dataIngest.py is in the same folder.\n")
        finally:
            # Upon completion, reset the process reference so a new job can be started
            self.process = None

    """
    This helper builds the command line to run dataIngest.py with the
    given configuration dictionary. It returns a list of strings suitable
    for passing to subprocess.Popen().
    """
    def _build_command(self, config: dict) -> list[str]:
        # Extracts the tickers from the config, splitting by commas and stripping whitespace
        tickers = [t.strip() for t in config.get("tickers", "").split(",") if t.strip()]

        # Builds the command line to run dataIngest.py with the specified tickers, years, output directory, and S3 prefix.
        command = [
            sys.executable,
            str(SCRIPT_PATH),
            "--tickers", *tickers,
            "--years", str(config.get("years", "10")).strip(),
            "--output-dir", config.get("output_dir", "data/raw").strip(),
            "--s3-prefix", config.get("s3_prefix", "raw").strip(),
        ]

        # If a bucket is specified in the config, add it to the command line
        bucket = (config.get("bucket") or "").strip()
        if bucket:
            command += ["--bucket", bucket]

        return command


def main():
    screen = webview.screens[0] # Get the primary screen
    window_width = int(screen.width * 0.8)   # 80% of screen width
    window_height = int(screen.height * 0.8)  # 80% of screen height

    # Create an instance of the Api class, which will serve as the bridge between Python and JavaScript
    api = Api()

    # Create a webview window that loads the dashboard.html file and sets the Api instance as the JavaScript API
    window = webview.create_window(
        "Stock Data Ingestion Application",
        str(HTML_PATH),
        js_api=api,
        width=window_width,
        height=window_height,
        resizable=True,
    )

    # Set the window reference in the Api instance so it can call window.evaluate_js(...) later
    api.set_window(window)
    webview.start()


if __name__ == "__main__":
    main()
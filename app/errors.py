import os
import logging
from logging.handlers import RotatingFileHandler
from flask import jsonify

def register_error_handlers(app):
    os.makedirs("logs", exist_ok=True)
    handler = RotatingFileHandler("logs/app.log", maxBytes=100000, backupCount=10)
    handler.setLevel(getattr(logging, app.config.get("LOG_LEVEL", "INFO")))
    handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    app.logger.addHandler(handler)

    @app.errorhandler(404)
    def not_found(e):
        return jsonify(error="Resource not found"), 404

    @app.errorhandler(500)
    def server_error(e):
        app.logger.exception("Unhandled exception")
        return jsonify(error="Internal server error"), 500
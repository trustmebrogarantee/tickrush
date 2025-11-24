# app/blueprints/ticks/__init__.py
from flask import Blueprint
from .routes import ticks_bp
from .tasks import start_background_sync

def create_ticks_blueprint():
    bp = Blueprint("ticks", __name__, url_prefix="/ticks")

    # Register sub-blueprint with all routes
    bp.register_blueprint(ticks_bp)

    # Auto-start background sync when the app loads this blueprint
    @bp.record_once
    def on_load(state):
        app = state.app
        with app.app_context():
            start_background_sync()

    return bp
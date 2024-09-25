from flask import Flask

def create_app():
    app = Flask(__name__)
    from .app import webapp_blueprint
    app.register_blueprint(webapp_blueprint)
    return app
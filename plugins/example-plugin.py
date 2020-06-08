from airflow.plugins_manager import AirflowPlugin
from flask_admin.base import MenuLink

"""
Look for the Astronomer tab in the UI.
"""
airflow_plugins_ml = MenuLink(
    category='Astronomer',
    name='Airflow-Plugins',
    url='https://github.com/airflow-plugins/')

astro_docs_ml = MenuLink(
    category='Astronomer',
    name='Astronomer Docs',
    url='https://www.astronomer.io/docs/')

astro_guides_ml = MenuLink(
    category='Astronomer',
    name='Airflow Guides',
    url='https://www.astronomer.io/guides/')


class AstroLinksPlugin(AirflowPlugin):
    name = 'astronomer_menu_links'
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    menu_links = [airflow_plugins_ml, astro_docs_ml, astro_guides_ml]
    appbuilder_views = []
    appbuilder_menu_items = [
        {
            "name": ml.name,
            "category": ml.category,
            "category_icon": "fa-rocket",
            "href": ml.url,
        } for ml in menu_links
    ]
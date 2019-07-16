"""web URL Configuration"""
from django.conf.urls import url
from django.contrib import admin
from django.urls import include, path
from rest_framework import routers

from web.api.views import ProjectViewSet, EmployeeViewSet


class DPEView(routers.APIRootView):
    """
    The Datapunt Projects and Employees (DPE) API serves all our project id/names and assigned employee (user)names, including the current statuses of these 2.
    The data is coming from a daily export from our hour administation system Timetell: https://web-amsterdamois.timetell.online.
    This api is used to lookup and assign the proper authentication to each employee (using Keycloak) and is used as a trigger for our project management system.
    """


class DPERouter(routers.DefaultRouter):
    APIRootView = DPEView


dpe_router = DPERouter()
dpe_router.register(r'projects', ProjectViewSet)
dpe_router.register(r'employees', EmployeeViewSet)

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^api/', include(dpe_router.urls)),
    url(r'^api-auth/', include('rest_framework.urls',
                               namespace='rest_framework')),
]

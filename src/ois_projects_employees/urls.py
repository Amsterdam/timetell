"""web URL Configuration"""
from django.conf.urls import url
from django.contrib import admin
from django.urls import include
from rest_framework import routers

from .api.views import ProjectViewSet, EmployeeViewSet


class OISProjectsEmployeesView(routers.APIRootView):
    """
    The OIS Projects and Employees API serves all our project id/names
    and assigned employee (user)names, including the current statuses.
    The data is coming from a daily export from our hour administation system:
    Timetell: https://web-amsterdamois.timetell.online
    This api is used to lookup and assign the proper authentication
    to each employee (using Keycloak) and is used as a trigger for
    our project management system.
    """


class OISProjectsEmployeesRouter(routers.DefaultRouter):
    APIRootView = OISProjectsEmployeesView


dpe_router = OISProjectsEmployeesRouter()
dpe_router.register(r'projects', ProjectViewSet)
dpe_router.register(r'employees', EmployeeViewSet)

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^api/', include(dpe_router.urls)),
    url(r'^api-auth/', include('rest_framework.urls',
                               namespace='rest_framework')),
]

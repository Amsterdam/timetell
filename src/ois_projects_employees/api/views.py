from rest_framework import viewsets
from rest_framework import filters
from django_filters.rest_framework import DjangoFilterBackend
from . import models
from . import serializers


class ProjectViewSet(viewsets.ModelViewSet):
    queryset = models.Prj.objects.all()
    serializer_class = serializers.ProjectDetailSerializer
    filter_backends = (filters.SearchFilter, DjangoFilterBackend,
                       filters.OrderingFilter,)
    search_fields = ('nr', 'name')
    filterset_fields = ('nr', 'status',)


class EmployeeViewSet(viewsets.ModelViewSet):
    queryset = models.Emp.objects.all()
    serializer_class = serializers.EmployeeSerializer
    filter_backends = (filters.SearchFilter, DjangoFilterBackend,)
    search_fields = ('firstname', 'lastname', 'loginname',)
    filterset_fields = ('nonactive',)

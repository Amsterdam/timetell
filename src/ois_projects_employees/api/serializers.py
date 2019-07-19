from rest_framework import serializers

from . import models


class EmployeeSummarySerializer(serializers.HyperlinkedModelSerializer):
    uri = serializers.HyperlinkedIdentityField(view_name="emp-detail")

    class Meta:
        model = models.Emp
        fields = ('uri', 'name')


class PrjLinkSerializer(serializers.HyperlinkedModelSerializer):
    employee = EmployeeSummarySerializer(source='emp')

    class Meta:
        model = models.PrjLink
        fields = ('employee', 'prjleader',)
        # depth = 1


class ProjectSummarySerializer(serializers.ModelSerializer):
    # Url needs a model name + '-detail' and a source in detail serializer with
    # the basename (is registered name from urls.py to work
    uri = serializers.HyperlinkedIdentityField(view_name="prj-detail")

    class Meta:
        model = models.Prj
        fields = ('uri',  'prj_id', 'nr', 'name')


class ProjectDetailSerializer(serializers.HyperlinkedModelSerializer):
    uri = serializers.HyperlinkedIdentityField(view_name='prj-detail',
                                               source='projects',
                                               read_only=True)
    employees = PrjLinkSerializer(source='prjlink_set',
                                  many=True, read_only=True)

    class Meta:
        model = models.Prj
        fields = ('uri', 'prj_id', 'parent', 'nr', 'name', 'status',
                  'fromdate', 'todate', 'employees')


class EmployeeSerializer(serializers.HyperlinkedModelSerializer):
    projects = ProjectSummarySerializer(many=True, read_only=True)

    class Meta:
        model = models.Emp
        fields = ('emp_id', 'firstname', 'middlename', 'lastname',
                  'nonactive', 'loginname', 'email1', 'mobile1',
                  'projects')

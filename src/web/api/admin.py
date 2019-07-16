from django.contrib import admin

from web.api.models import Prj, Emp, PrjLink

admin.site.register(Prj)
admin.site.register(Emp)
admin.site.register(PrjLink)
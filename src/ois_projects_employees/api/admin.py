from django.contrib import admin

from .models import Prj, Emp, PrjLink

admin.site.register(Prj)
admin.site.register(Emp)
admin.site.register(PrjLink)

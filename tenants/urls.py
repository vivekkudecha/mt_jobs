from django.urls import path

from tenants.views import TenantDetailView, TenantListCreateView

urlpatterns = [
    path("tenants/", TenantListCreateView.as_view(), name="tenant-list-create"),
    path("tenants/<uuid:pk>/", TenantDetailView.as_view(), name="tenant-detail"),
]

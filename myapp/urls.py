from django.urls import path
from . import views
from django.conf import settings
from django.conf.urls.static import static
from .views import Download_excel,Download_edi_Custodial_csv,Download_edi_Custodial_xlsx
urlpatterns = [
    path('signup/', views.SignupView.as_view(), name='signup'),
    path('login/', views.LoginView.as_view(), name='login'),
    path('upload-file/', views.FileUploadView.as_view(), name='upload_file'),
    path('download-file/', views.download_file, name='download_file'),
    path('download-excel-file/', views.download_excel_file, name='download_file'),
    path('files/', views.FilesListView.as_view(), name='all_files'),
    path('files_filter/', views.FilesFilterView.as_view(), name='files_by_date'),
    path('archived_files/', views.ArchiveListView.as_view(), name='all_files'),
    path('archived_files_filter/', views.ArchiveFilterView.as_view(), name='files_by_date'),
    path('download-input-file/<int:file_id>/', views.download_input_file, name='download_input_file'),
    path('send-otp/', views.SendOTPView.as_view(), name='send_otp'),
    path('otp-login/', views.OTPLoginView.as_view(), name='otp_login'),
    path('download_edi_excel',Download_excel),
    path('download_edi_to_excel_csv',Download_edi_Custodial_csv.as_view()),
    path('download_edi_to_excel_xlsx',Download_edi_Custodial_xlsx.as_view()),
    path('download_inventory_report',views.export_inventory_data),
    path('get_members',views.GetMemberInfo.as_view()),
    path('get_count',views.Get_Count.as_view()),
    path('search_member',views.search_employee),
    path('add_member',views.add_member),
    path('update_member',views.UpdateMemberInfo.as_view())
]


if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
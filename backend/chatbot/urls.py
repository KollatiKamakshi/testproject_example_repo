from django.urls import path
from .views import top_products, chatbot_view

urlpatterns = [
    path('top-products/', top_products),
    path('chat/', chatbot_view, name='chatbot'),
]


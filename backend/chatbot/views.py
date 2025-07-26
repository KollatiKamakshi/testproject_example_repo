import os
import json
from decouple import config
from django.shortcuts import render
from django.http import JsonResponse

from openai import OpenAI
from django.views.decorators.csrf import csrf_exempt

api_key = config("GROQ_API_KEY")

client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=api_key
)

# Example view for top products (your existing one)
from .models import Product
from rest_framework.decorators import api_view

def top_products(request):
    top_products = Product.objects.all()[:5]
    data = [{"name": p.name, "price": p.price} for p in top_products]
    return JsonResponse({"top_products": data})

@csrf_exempt
def chatbot_view(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            user_message = data.get('message')

            response = client.chat.completions.create(
                model="llama3-70b-8192",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant for EasyTradeZir."},
                    {"role": "user", "content": user_message}
                ]
            )

            answer = response.choices[0].message.content

            return JsonResponse({'response': answer})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    else:
        return JsonResponse({'error': 'POST request required.'}, status=400)

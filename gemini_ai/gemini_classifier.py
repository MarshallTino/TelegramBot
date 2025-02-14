# gemini_ai/gemini_classifier.py

import google.generativeai as genai

def gemini_classify(text, api_key=""):
    """
    Clasifica el mensaje usando Gemini AI.
    Si falla (por ejemplo 429), retornamos True como fallback para no bloquear.
    """
    if not api_key:
        # Si no tenemos api_key, devolvemos True (procesar igual)
        return True
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-pro")
        prompt = f"Clasifica como RELEVANTE sólo si menciona nuevo token:\n'{text}'"
        resp = model.generate_content(prompt)
        return "relevante" in resp.text.strip().lower()
    except Exception as e:
        print(f"⚠️ Error en gemini_classify: {e}. Fallback => True")
        return True

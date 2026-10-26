from config import MODEL_ROUTES

def resolve_model(model: str) -> str:
    if model in MODEL_ROUTES:
        return MODEL_ROUTES[model]
    # 允许直接传 Ollama 模型名
    return model
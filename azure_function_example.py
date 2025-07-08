import azure.functions as func
import json
import logging
import os
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, List, Any

# Configuración de la aplicación
app = func.FunctionApp()

@app.function_name(name="ProcessTransactionData")
@app.route(route="process-transaction", auth_level=func.AuthLevel.FUNCTION)
def process_transaction_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function para procesar datos transaccionales y generar cálculos ficticios
    Similar al analizador de riesgo del sistema Denarius
    """
    logging.info('Procesando solicitud de análisis transaccional')
    
    try:
        # Obtener datos de la solicitud
        req_body = req.get_json()
        
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "No se proporcionaron datos"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Validar estructura de datos requerida
        required_fields = ['tenant_id', 'client_id', 'account_number', 'transaction_amount', 'causal_code']
        missing_fields = [field for field in required_fields if field not in req_body]
        
        if missing_fields:
            return func.HttpResponse(
                json.dumps({"error": f"Campos faltantes: {missing_fields}"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Extraer datos de la transacción
        transaction_data = {
            'tenant_id': req_body['tenant_id'],
            'client_id': req_body['client_id'],
            'account_number': req_body['account_number'],
            'transaction_amount': float(req_body['transaction_amount']),
            'causal_code': req_body['causal_code'],
            'currency': req_body.get('currency', 'USD'),
            'channel': req_body.get('channel', 'WEB'),
            'timestamp': req_body.get('timestamp', datetime.now().isoformat())
        }
        
        # Obtener datos históricos calculados (simulado)
        historical_data = get_historical_data(transaction_data['account_number'])
        
        # Realizar cálculos ficticios de riesgo
        risk_analysis = calculate_risk_metrics(transaction_data, historical_data)
        
        # Preparar respuesta
        response_data = {
            'transaction_id': f"TXN_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'account_number': transaction_data['account_number'],
            'risk_score': risk_analysis['risk_score'],
            'risk_level': risk_analysis['risk_level'],
            'calculated_metrics': risk_analysis['metrics'],
            'recommendations': risk_analysis['recommendations'],
            'processing_timestamp': datetime.now().isoformat()
        }
        
        # Log para monitoreo
        logging.info(f"Transacción procesada: {response_data['transaction_id']}, Risk Score: {risk_analysis['risk_score']}")
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error procesando transacción: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error interno: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

def get_historical_data(account_number: str) -> Dict[str, Any]:
    """
    Simula la obtención de datos históricos calculados desde Fabric/Synapse
    En implementación real, esto consultaría la base de datos o API de Fabric
    """
    # Datos ficticios basados en el patrón del sistema Denarius
    historical_data = {
        'avg_transaction_amount': np.random.uniform(100, 1000),
        'std_transaction_amount': np.random.uniform(50, 200),
        'transaction_count_30d': np.random.randint(10, 100),
        'avg_daily_transactions': np.random.uniform(1, 10),
        'max_transaction_amount': np.random.uniform(1000, 5000),
        'min_transaction_amount': np.random.uniform(10, 100),
        'account_age_days': np.random.randint(30, 1000),
        'last_transaction_date': (datetime.now() - timedelta(days=np.random.randint(1, 30))).isoformat(),
        'common_channels': ['WEB', 'MOBILE', 'ATM'],
        'common_causals': ['TRANSFER', 'PAYMENT', 'WITHDRAWAL']
    }
    
    return historical_data

def calculate_risk_metrics(transaction_data: Dict[str, Any], historical_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcula métricas de riesgo ficticias basadas en los datos transaccionales e históricos
    """
    amount = transaction_data['transaction_amount']
    avg_amount = historical_data['avg_transaction_amount']
    std_amount = historical_data['std_transaction_amount']
    
    # Cálculos ficticios de riesgo
    metrics = {
        'amount_deviation': abs(amount - avg_amount) / std_amount if std_amount > 0 else 0,
        'amount_ratio': amount / avg_amount if avg_amount > 0 else 1,
        'frequency_score': min(historical_data['transaction_count_30d'] / 30, 10),
        'time_since_last': (datetime.now() - datetime.fromisoformat(historical_data['last_transaction_date'].replace('Z', '+00:00').replace('+00:00', ''))).days,
        'account_maturity': min(historical_data['account_age_days'] / 365, 5)
    }
    
    # Calcular score de riesgo compuesto
    risk_components = [
        metrics['amount_deviation'] * 0.3,
        (metrics['amount_ratio'] - 1) * 0.25 if metrics['amount_ratio'] > 1 else 0,
        (10 - metrics['frequency_score']) * 0.2,
        min(metrics['time_since_last'] / 30, 1) * 0.15,
        (5 - metrics['account_maturity']) * 0.1
    ]
    
    risk_score = sum(risk_components) * 100
    risk_score = max(0, min(100, risk_score))  # Normalizar entre 0-100
    
    # Determinar nivel de riesgo
    if risk_score < 30:
        risk_level = "BAJO"
        recommendations = ["Transacción normal", "Continuar monitoreo estándar"]
    elif risk_score < 70:
        risk_level = "MEDIO"
        recommendations = ["Revisar patrones", "Monitoreo adicional recomendado"]
    else:
        risk_level = "ALTO"
        recommendations = ["Revisión manual requerida", "Posible transacción fraudulenta"]
    
    return {
        'risk_score': round(risk_score, 2),
        'risk_level': risk_level,
        'metrics': metrics,
        'recommendations': recommendations
    }

@app.function_name(name="GetAccountMetrics")
@app.route(route="account/{account_number}/metrics", auth_level=func.AuthLevel.FUNCTION)
def get_account_metrics(req: func.HttpRequest) -> func.HttpResponse:
    """
    Endpoint para obtener métricas calculadas de una cuenta específica
    """
    logging.info('Obteniendo métricas de cuenta')
    
    try:
        account_number = req.route_params.get('account_number')
        
        if not account_number:
            return func.HttpResponse(
                json.dumps({"error": "Número de cuenta requerido"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Obtener métricas históricas
        historical_data = get_historical_data(account_number)
        
        # Agregar métricas adicionales calculadas
        additional_metrics = {
            'risk_profile': calculate_account_risk_profile(historical_data),
            'behavioral_patterns': analyze_behavioral_patterns(historical_data),
            'anomaly_indicators': detect_anomaly_indicators(historical_data)
        }
        
        response_data = {
            'account_number': account_number,
            'historical_metrics': historical_data,
            'calculated_metrics': additional_metrics,
            'last_updated': datetime.now().isoformat()
        }
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error obteniendo métricas: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Error interno: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

def calculate_account_risk_profile(historical_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcula el perfil de riesgo de la cuenta
    """
    avg_amount = historical_data['avg_transaction_amount']
    transaction_count = historical_data['transaction_count_30d']
    account_age = historical_data['account_age_days']
    
    # Clasificación de perfil de riesgo
    if avg_amount > 1000 and transaction_count > 50:
        profile = "ALTO_VOLUMEN"
        risk_factor = 0.8
    elif avg_amount < 200 and transaction_count < 10:
        profile = "BAJO_VOLUMEN"
        risk_factor = 0.3
    else:
        profile = "VOLUMEN_MEDIO"
        risk_factor = 0.5
    
    # Ajustar por antigüedad de cuenta
    if account_age < 90:
        risk_factor += 0.2
        profile += "_NUEVA"
    elif account_age > 365:
        risk_factor -= 0.1
        profile += "_ESTABLECIDA"
    
    return {
        'profile_type': profile,
        'risk_factor': round(risk_factor, 2),
        'stability_score': min(account_age / 365 * 100, 100)
    }

def analyze_behavioral_patterns(historical_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analiza patrones de comportamiento de la cuenta
    """
    return {
        'transaction_regularity': np.random.uniform(0.5, 1.0),
        'amount_consistency': np.random.uniform(0.3, 0.9),
        'channel_preference': np.random.choice(historical_data['common_channels']),
        'time_pattern': np.random.choice(['DIURNO', 'NOCTURNO', 'MIXTO']),
        'seasonal_variation': np.random.uniform(0.1, 0.5)
    }

def detect_anomaly_indicators(historical_data: Dict[str, Any]) -> List[str]:
    """
    Detecta indicadores de anomalías
    """
    indicators = []
    
    if historical_data['max_transaction_amount'] > historical_data['avg_transaction_amount'] * 5:
        indicators.append("TRANSACCION_ATIPICA_ALTA")
    
    if historical_data['transaction_count_30d'] > 80:
        indicators.append("FRECUENCIA_ALTA")
    
    if historical_data['account_age_days'] < 30:
        indicators.append("CUENTA_NUEVA")
    
    return indicators if indicators else ["COMPORTAMIENTO_NORMAL"]

@app.function_name(name="HealthCheck")
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Endpoint de verificación de salud del servicio
    """
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0',
        'environment': os.getenv('AZURE_FUNCTIONS_ENVIRONMENT', 'development')
    }
    
    return func.HttpResponse(
        json.dumps(health_status),
        status_code=200,
        mimetype="application/json"
    )


import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(
    page_title="Трафик Прогноз",
    layout="wide"
)

st.title("Система прогнозирования трафика")
st.markdown("---")

@st.cache_data
def load_data():
    df = pd.read_csv('traffic_full_30min.csv', parse_dates=['timestamp'])
    return df

@st.cache_resource
def train_model(df):
    # Подготовка данных
    weather_mapping = {'clear': 0, 'cloudy': 1, 'rain': 2, 'fog': 3}
    df['weather_encoded'] = df['weather_type'].map(weather_mapping)
    
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['month'] = df['timestamp'].dt.month
    
    features = ['hour', 'day_of_week', 'month', 'temperature', 'precipitation', 
                'intensity_30min', 'cars', 'trucks', 'busses', 'weather_encoded']
    target = 'avg_speed'
    
    df_model = df[features + [target]].dropna()
    X = df_model[features]
    y = df_model[target]
    
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    return model, df, features

with st.spinner("Загрузка данных и обучение модели..."):
    df = load_data()
    model, df, features = train_model(df)

last_row = df.iloc[-1]
last_timestamp = last_row['timestamp']

with st.sidebar:
    st.header("Текущая ситуация")
    st.metric("Скорость", f"{last_row['avg_speed']:.1f} км/ч")
    st.metric("Температура", f"{last_row['temperature']}°C")
    st.metric("Осадки", f"{last_row['precipitation']} мм")
    st.metric("Машин", f"{last_row['cars']}")
    st.metric("Грузовиков", f"{last_row['trucks']}")
    st.metric("Автобусов", f"{last_row['busses']}")
    st.metric("Погода", last_row['weather_type'])
    
    st.markdown("---")
    st.caption(f"Последние данные: {last_timestamp.strftime('%Y-%m-%d %H:%M')}")

tab1, tab2, tab3 = st.tabs(["Прогноз", "Анализ данных", "О модели"])

with tab1:
    st.header("Прогноз трафика")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Период прогноза")
        
        period_type = st.radio(
            "Выберите период:",
            ["30 минут", "60 минут", "120 минут", "Свой вариант"]
        )
        
        if period_type == "30 минут":
            minutes = 30
        elif period_type == "60 минут":
            minutes = 60
        elif period_type == "120 минут":
            minutes = 120
        else:
            minutes = st.number_input("Введите количество минут:", min_value=1, max_value=1440, value=60)
        
        st.subheader("Параметры (можно изменить)")
        
        use_current = st.checkbox("Использовать текущие данные", value=True)
        
        if use_current:
            temp = last_row['temperature']
            precip = last_row['precipitation']
            intensity = last_row['intensity_30min']
            cars = last_row['cars']
            trucks = last_row['trucks']
            busses = last_row['busses']
            weather = last_row['weather_type']
        else:
            col_a, col_b = st.columns(2)
            with col_a:
                temp = st.number_input("Температура (°C):", value=float(last_row['temperature']))
                precip = st.number_input("Осадки (мм):", value=float(last_row['precipitation']))
                intensity = st.number_input("Интенсивность:", value=int(last_row['intensity_30min']))
            with col_b:
                cars = st.number_input("Легковые:", value=int(last_row['cars']))
                trucks = st.number_input("Грузовики:", value=int(last_row['trucks']))
                busses = st.number_input("Автобусы:", value=int(last_row['busses']))
                weather = st.selectbox("Погода:", ['clear', 'cloudy', 'rain', 'fog'])
        
        predict_button = st.button("Сделать прогноз", type="primary", use_container_width=True)
    
    with col2:
        st.subheader("Результат")
        
        if predict_button:
            weather_map = {'clear': 0, 'cloudy': 1, 'rain': 2, 'fog': 3}
            weather_encoded = weather_map[weather]
            
            future_time = last_timestamp + pd.Timedelta(minutes=minutes)
            
            future_data = pd.DataFrame({
                'hour': [future_time.hour],
                'day_of_week': [future_time.dayofweek],
                'month': [future_time.month],
                'temperature': [temp],
                'precipitation': [precip],
                'intensity_30min': [intensity],
                'cars': [cars],
                'trucks': [trucks],
                'busses': [busses],
                'weather_encoded': [weather_encoded]
            })
            
            predicted_speed = model.predict(future_data)[0]
            
            if predicted_speed >= 60:
                status = "СВОБОДНЫЙ"
                color = "green"
                recommendation = "Дороги свободны, можно ехать!"
            elif predicted_speed >= 40:
                status = "УМЕРЕННЫЙ"
                color = "orange"
                recommendation = "Возможны небольшие задержки"
            elif predicted_speed >= 20:
                status = "ПЛОТНЫЙ"
                color = "orange"
                recommendation = "Ожидаются пробки, выезжайте заранее"
            else:
                status = "ПРОБКА"
                color = "red"
                recommendation = "Серьезные пробки, лучше отложить поездку"
        
            st.metric(
                label=f"Прогноз через {minutes} минут",
                value=f"{predicted_speed:.1f} км/ч",
                delta=f"{predicted_speed - last_row['avg_speed']:.1f} км/ч"
            )
            
            st.markdown(f"### {status}")
            st.info(f"{recommendation}")
            
            with st.expander("Подробности прогноза"):
                st.write(f"**Время прогноза:** {future_time.strftime('%Y-%m-%d %H:%M:%S')}")
                st.write(f"**Текущая скорость:** {last_row['avg_speed']:.1f} км/ч")
                
                diff = predicted_speed - last_row['avg_speed']
                if diff > 0:
                    st.success(f"Скорость увеличится на {diff:.1f} км/ч")
                elif diff < 0:
                    st.error(f"Скорость снизится на {abs(diff):.1f} км/ч")
                else:
                    st.info("Скорость не изменится")
                
                st.write("**Использованные параметры:**")
                st.write(f"- Температура: {temp}°C")
                st.write(f"- Осадки: {precip} мм")
                st.write(f"- Интенсивность: {intensity}")
                st.write(f"- Машин: {cars}, Грузовиков: {trucks}, Автобусов: {busses}")
                st.write(f"- Погода: {weather}")
    
    st.markdown("---")
    st.subheader("Сравнение прогнозов")
    
    col1, col2, col3 = st.columns(3)
    
    for i, (mins, col, color) in enumerate([(30, col1, "green"), (60, col2, "orange"), (120, col3, "red")]):
        future_time = last_timestamp + pd.Timedelta(minutes=mins)
        future_data = pd.DataFrame({
            'hour': [future_time.hour],
            'day_of_week': [future_time.dayofweek],
            'month': [future_time.month],
            'temperature': [last_row['temperature']],
            'precipitation': [last_row['precipitation']],
            'intensity_30min': [last_row['intensity_30min']],
            'cars': [last_row['cars']],
            'trucks': [last_row['trucks']],
            'busses': [last_row['busses']],
            'weather_encoded': [last_row['weather_encoded']]
        })
        speed = model.predict(future_data)[0]
        
        with col:
            st.metric(
                label=f"Через {mins} минут",
                value=f"{speed:.1f} км/ч",
                delta=f"{speed - last_row['avg_speed']:.1f} км/ч",
                delta_color="normal"
            )
            
            if speed >= 60:
                st.markdown("Свободно")
            elif speed >= 40:
                st.markdown("Умеренно")
            elif speed >= 20:
                st.markdown("Плотно")
            else:
                st.markdown("Пробка")

with tab2:
    st.header("Анализ данных")

    chart_type = st.selectbox(
        "Выберите график:",
        ["Скорость во времени", "Скорость по часам", "Распределение скоростей", 
         "Скорость по дням недели", "Скорость по погоде", "Корреляция"]
    )
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    if chart_type == "Скорость во времени":
        plot_data = df.tail(500)
        ax.plot(plot_data['timestamp'], plot_data['avg_speed'], linewidth=1, alpha=0.7, color='blue')
        ax.set_title('Средняя скорость во времени', fontsize=14, fontweight='bold')
        ax.set_xlabel('Время')
        ax.set_ylabel('Скорость (км/ч)')
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
    elif chart_type == "Скорость по часам":
        hourly_speed = df.groupby('hour')['avg_speed'].mean()
        ax.bar(hourly_speed.index, hourly_speed.values, color='skyblue', edgecolor='black')
        ax.set_title('Средняя скорость по часам суток', fontsize=14, fontweight='bold')
        ax.set_xlabel('Час')
        ax.set_ylabel('Скорость (км/ч)')
        ax.grid(True, alpha=0.3, axis='y')
        
    elif chart_type == "Распределение скоростей":
        ax.hist(df['avg_speed'], bins=50, edgecolor='black', alpha=0.7, color='lightgreen')
        ax.set_title('Распределение средней скорости', fontsize=14, fontweight='bold')
        ax.set_xlabel('Скорость (км/ч)')
        ax.set_ylabel('Частота')
        ax.grid(True, alpha=0.3, axis='y')
        
    elif chart_type == "Скорость по дням недели":
        weekday_speed = df.groupby('day_of_week')['avg_speed'].mean()
        days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
        ax.bar(days, weekday_speed.values, color='coral', edgecolor='black')
        ax.set_title('Средняя скорость по дням недели', fontsize=14, fontweight='bold')
        ax.set_xlabel('День недели')
        ax.set_ylabel('Скорость (км/ч)')
        ax.grid(True, alpha=0.3, axis='y')
        
    elif chart_type == "Скорость по погоде":
        df.boxplot(column='avg_speed', by='weather_type', ax=ax)
        ax.set_title('Скорость в зависимости от погоды', fontsize=14, fontweight='bold')
        ax.set_xlabel('Тип погоды')
        ax.set_ylabel('Скорость (км/ч)')
        plt.suptitle('')
        
    elif chart_type == "Корреляция":
        numeric_cols = ['avg_speed', 'temperature', 'precipitation', 'intensity_30min', 
                        'cars', 'trucks', 'busses', 'hour', 'day_of_week']
        corr = df[numeric_cols].corr()
        sns.heatmap(corr, annot=True, fmt='.2f', cmap='coolwarm', ax=ax)
        ax.set_title('Корреляционная матрица', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

with tab3:
    st.header("Информация о модели")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Метрики модели")
        
        df_model = df[features + ['avg_speed']].dropna()
        X = df_model[features]
        y = df_model['avg_speed']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        y_pred = model.predict(X_test)
        
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        st.metric("MAE (средняя ошибка)", f"{mae:.2f} км/ч")
        st.metric("R² (точность)", f"{r2:.3f}")
        st.progress(r2)
        
    with col2:
        st.subheader("Важность признаков")
        
        importance_df = pd.DataFrame({
            'Признак': features,
            'Важность': model.feature_importances_
        }).sort_values('Важность', ascending=True)
        
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.barh(importance_df['Признак'], importance_df['Важность'], color='teal')
        ax.set_xlabel('Важность')
        ax.set_title('Важность признаков для прогноза')
        st.pyplot(fig)
        plt.close()
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Статистика данных")
        st.write(f"**Всего записей:** {len(df):,}")
        st.write(f"**Период:** {df['timestamp'].min().strftime('%Y-%m-%d')} - {df['timestamp'].max().strftime('%Y-%m-%d')}")
        st.write(f"**Средняя скорость:** {df['avg_speed'].mean():.1f} ± {df['avg_speed'].std():.1f} км/ч")
        st.write(f"**Мин скорость:** {df['avg_speed'].min():.1f} км/ч")
        st.write(f"**Макс скорость:** {df['avg_speed'].max():.1f} км/ч")
        
    with col2:
        st.subheader("Погодные условия")
        st.write(f"**Средняя температура:** {df['temperature'].mean():.1f}°C")
        st.write(f"**Средние осадки:** {df['precipitation'].mean():.2f} мм")
        
        weather_counts = df['weather_type'].value_counts()
        for weather, count in weather_counts.items():
            st.write(f"**{weather}:** {count} записей ({count/len(df)*100:.1f}%)")
    
    st.markdown("---")
    st.info("Для более точного прогноза используйте актуальные данные о погоде и трафике")
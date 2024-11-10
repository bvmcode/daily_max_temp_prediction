from sklearn.ensemble import ExtraTreesRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
import pickle
import optuna
import pandas as pd


def train_model_random_forest(df, df_obs):
    df['forecast_date'] = pd.to_datetime(df['forecast_date']).dt.strftime("%Y-%m-%d")
    df_obs['forecast_date'] = pd.to_datetime(df_obs['forecast_date']).dt.strftime("%Y-%m-%d")
    df_merged = df.merge(df_obs, on='forecast_date', how='inner')
    df_merged['month'] = pd.to_datetime(df_merged['forecast_date']).dt.month
    df_merged = df_merged.dropna()
    target = df_merged["max_temp_f"]
    df_notarget = df_merged.drop(columns=['forecast_date', 'max_temp_f'])
    X_train, X_test, y_train, y_test = train_test_split(df_notarget, target, test_size=0.1)
    scaler = StandardScaler()
    X_train_scaled = scaler.transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    def objective(trial):
        param = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 300),
            'max_depth': trial.suggest_int('max_depth', 3, 20),
            'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
            'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 20),
            'max_features': trial.suggest_categorical('max_features', ['sqrt', 'log2', None]),
            'bootstrap': trial.suggest_categorical('bootstrap', [True, False])
        }
        reg = ExtraTreesRegressor(**param)
        reg.fit(X_train_scaled, y_train)
        y_pred = reg.predict(X_test_scaled)
        mse = mean_squared_error(y_test, y_pred)
        return mse
    
    study = optuna.create_study(direction='minimize')  # Minimizing MSE
    study.optimize(objective, n_trials=500)
    best_params = study.best_params
    reg = ExtraTreesRegressor(**best_params)
    reg.fit(X_train_scaled, y_train)
    return reg, scaler


if __name__=='__main__':
    df = pd.read_csv('features.csv')
    df_obs = pd.read_csv('labels.csv')
    model, scaler = train_model_random_forest(df, df_obs)
    pickle.dump(scaler, open('scaler.sav', 'wb'))
    pickle.dump(model, open('model.pkl', 'wb'))

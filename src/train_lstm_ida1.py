# src/train_lstm_ida1.py

import os
import numpy as np
import pandas as pd

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader


# ===========================
# 1. PyTorch Dataset
# ===========================

class SequenceDataset(Dataset):
    def __init__(self, X, y, seq_len: int):
        """
        X: np.ndarray [N, num_features]
        y: np.ndarray [N]
        Δημιουργεί δείγματα (sequence -> target) με μήκος sequence seq_len.
        """
        self.X = X
        self.y = y
        self.seq_len = seq_len

    def __len__(self):
        return len(self.X) - self.seq_len

    def __getitem__(self, idx):
        x_seq = self.X[idx : idx + self.seq_len]       # [seq_len, num_features]
        y_target = self.y[idx + self.seq_len]          # scalar
        return torch.from_numpy(x_seq).float(), torch.tensor(y_target).float()


# ===========================
# 2. LSTM Model
# ===========================

class LSTMRegressor(nn.Module):
    def __init__(self, input_size: int, hidden_size: int = 64, num_layers: int = 2, dropout: float = 0.2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        """
        x: [batch, seq_len, input_size]
        """
        out, _ = self.lstm(x)     # out: [batch, seq_len, hidden_size]
        last = out[:, -1, :]      # για regression παίρνουμε το τελευταίο time step
        pred = self.fc(last)      # [batch, 1]
        return pred.squeeze(-1)   # [batch]


# ===========================
# 3. Βοηθητικές συναρτήσεις
# ===========================

def train_one_epoch(model, loader, optimizer, criterion, device):
    model.train()
    total_loss = 0.0
    for xb, yb in loader:
        xb = xb.to(device)
        yb = yb.to(device)

        optimizer.zero_grad()
        preds = model(xb)
        loss = criterion(preds, yb)
        loss.backward()
        optimizer.step()

        total_loss += loss.item() * len(xb)
    return total_loss / len(loader.dataset)


def evaluate(model, loader, criterion, device):
    model.eval()
    total_loss = 0.0
    preds_list = []
    targets_list = []
    with torch.no_grad():
        for xb, yb in loader:
            xb = xb.to(device)
            yb = yb.to(device)
            preds = model(xb)
            loss = criterion(preds, yb)
            total_loss += loss.item() * len(xb)
            preds_list.append(preds.cpu().numpy())
            targets_list.append(yb.cpu().numpy())
    y_pred = np.concatenate(preds_list)
    y_true = np.concatenate(targets_list)
    return total_loss / len(loader.dataset), y_true, y_pred


# ===========================
# 4. Main
# ===========================

def main():
    csv_path = "data/processed/idm_dataset_2024.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Δεν βρήκα το αρχείο: {csv_path}")

    df = pd.read_csv(csv_path, parse_dates=["DELIVERY_MTU"])

    # --- 4.1 Φιλτράρουμε μόνο IDA1 ---
    df = df[df["AUCTION"] == "IDA1"].copy()

    # --- 4.2 Ταξινόμηση χρονικά (πολύ σημαντικό!) ---
    df = df.sort_values("DELIVERY_MTU").reset_index(drop=True)

    # --- 4.3 Επιλογή στόχου ---
    target_col = "MCP"   # IDM price
    if target_col not in df.columns:
        raise KeyError(f"Η στήλη {target_col} δεν υπάρχει στο dataset.")

    # --- 4.4 Επιλογή features (όλα τα numeric εκτός του target) ---
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    feature_cols = [c for c in numeric_cols if c != target_col]

    print(f"Χρησιμοποιώ {len(feature_cols)} features:")
    print(feature_cols[:20], "...")

    X_all = df[feature_cols].to_numpy().astype(np.float32)
    y_all = df[target_col].to_numpy().astype(np.float32)

    # --- 4.5 Train / Test split 70/30 χρονικά ---
    n = len(df)
    split_idx = int(n * 0.7)
    X_train, X_test = X_all[:split_idx], X_all[split_idx:]
    y_train, y_test = y_all[:split_idx], y_all[split_idx:]

    # --- 4.6 Scaling (fit μόνο στο train!) ---
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # --- 4.7 Φτιάχνουμε sequence datasets ---
    seq_len = 24  # 24 ώρες ιστορικό
    train_dataset = SequenceDataset(X_train_scaled, y_train, seq_len=seq_len)
    test_dataset = SequenceDataset(X_test_scaled, y_test, seq_len=seq_len)

    train_loader = DataLoader(train_dataset, batch_size=64, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=64, shuffle=False)

    # --- 4.8 Ορισμός μοντέλου / optimizer κτλ ---
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = LSTMRegressor(input_size=len(feature_cols), hidden_size=64, num_layers=2, dropout=0.2)
    model.to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    criterion = nn.MSELoss()

    n_epochs = 30
    best_val_loss = float("inf")
    best_state = None

    for epoch in range(1, n_epochs + 1):
        train_loss = train_one_epoch(model, train_loader, optimizer, criterion, device)
        val_loss, y_true, y_pred = evaluate(model, test_loader, criterion, device)

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state = model.state_dict()

        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        mae = mean_absolute_error(y_true, y_pred)

        print(
            f"Epoch {epoch:02d} | "
            f"Train MSE: {train_loss:.4f} | "
            f"Test MSE: {val_loss:.4f} | "
            f"RMSE: {rmse:.4f} | MAE: {mae:.4f}"
        )

    # --- 4.9 Φορτώνουμε το καλύτερο μοντέλο (early best) ---
    if best_state is not None:
        model.load_state_dict(best_state)

    # --- 4.10 Τελική αξιολόγηση ---
    final_loss, y_true, y_pred = evaluate(model, test_loader, criterion, device)
    final_rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    final_mae = mean_absolute_error(y_true, y_pred)

    print("\n===== ΤΕΛΙΚΑ ΑΠΟΤΕΛΕΣΜΑΤΑ (IDA1 LSTM) =====")
    print(f"Test MSE : {final_loss:.4f}")
    print(f"Test RMSE: {final_rmse:.4f}")
    print(f"Test MAE : {final_mae:.4f}")

    # (προαιρετικά) σώζουμε το μοντέλο
    os.makedirs("models", exist_ok=True)
    torch.save(
        {
            "state_dict": model.state_dict(),
            "scaler_mean": scaler.mean_,
            "scaler_scale": scaler.scale_,
            "feature_cols": feature_cols,
            "seq_len": seq_len,
        },
        "models/lstm_ida1.pt",
    )
    print("Μοντέλο αποθηκευμένο στο models/lstm_ida1.pt")


if __name__ == "__main__":
    main()

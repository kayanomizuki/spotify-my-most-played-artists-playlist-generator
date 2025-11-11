# generator.py
# Spotify長期ストリーミング履歴(JSON)から
# 「よく聴くアーティスト順 → 各アーティスト内でよく聴く曲順」のプレイリストを作成し、
# 同一順序のCSVも保存します（UTF-8 with BOM / CRLF）。

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth


# ---------------------------
# アーティストパターン関連
# ---------------------------
def _load_artist_patterns(path: Optional[str]) -> List[str]:
    """UTF-8テキストからパターンを読み込む（1行1パターン、空行/先頭#は無視）。"""
    if not path:
        return []
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Artists filter file not found: {path}")

    patterns: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            patterns.append(s)
    print(f"[INFO] アーティストパターン数: {len(patterns)}（{os.path.basename(path)}）")
    return patterns


def _apply_artist_patterns(df: pd.DataFrame, patterns: List[str], mode: str) -> pd.DataFrame:
    """
    mode:
      - 'exclude' : 部分一致するアーティストを除外
      - 'include' : 部分一致するアーティストのみ採用
    大文字小文字は無視。
    """
    if not patterns:
        return df

    regex = "|".join([re.escape(p) for p in patterns])
    matched = df["artist"].astype(str).str.contains(
        regex, case=False, na=False)

    if mode == "include":
        before = len(df)
        df2 = df.loc[matched].copy()
        print(
            f"[INFO] アーティスト包含フィルタ: {before} → {len(df2)}（採用 {int(matched.sum())}）")
        return df2
    elif mode == "exclude":
        before = len(df)
        df2 = df.loc[~matched].copy()
        print(
            f"[INFO] アーティスト除外フィルタ: {before} → {len(df2)}（除外 {int(matched.sum())}）")
        return df2
    else:
        raise ValueError("artists_filter_mode must be 'exclude' or 'include'.")


# ---------------------------
# 1) 再生履歴の読み込み
# ---------------------------
def _load_history(source_dir: str) -> pd.DataFrame:
    print("[STEP 1/6] 再生履歴(JSON)の読み込みを開始...")

    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    files = sorted(
        os.path.join(source_dir, f)
        for f in os.listdir(source_dir)
        if f.lower().endswith(".json")
    )
    if not files:
        raise FileNotFoundError(f"No JSON files found in: {source_dir}")

    frames: List[pd.DataFrame] = []
    for path in files:
        try:
            part = pd.read_json(path, convert_dates=False)
            frames.append(part)
            print(f"[READ] {os.path.basename(path)} ... OK ({len(part)} 行)")
        except Exception as e:
            print(f"[WARN] 読み込み失敗: {path} ({e})")

    if not frames:
        raise RuntimeError("No readable JSON files.")

    df = pd.concat(frames, ignore_index=True)

    # 欠損列補完（フォーマット差異に寛容）
    for col in [
        "ts",
        "ms_played",
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
        "spotify_track_uri",
        "episode_name",
        "audiobook_title",
    ]:
        if col not in df.columns:
            df[col] = None

    # ts を datetime（UTC）
    try:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    except Exception:
        pass

    print(f"[INFO] 読み込み完了: 合計 {len(df)} 行")
    return df


# ---------------------------
# 2) 楽曲データへフィルタ
# ---------------------------
def _filter_music_rows(
    df: pd.DataFrame,
    min_play_ms: int,
    year_start: Optional[int],
    year_end: Optional[int],
) -> pd.DataFrame:
    print("[STEP 2/6] フィルタリングを実行中...")

    # Podcast / Audiobook を除外
    df = df[(df["episode_name"].isna()) & (
        df["audiobook_title"].isna())].copy()

    # 必須メタ（曲名・アーティスト名）
    df = df[
        df["master_metadata_track_name"].notna()
        & df["master_metadata_album_artist_name"].notna()
    ].copy()

    # 最小再生時間
    df = df[df["ms_played"].fillna(0) >= int(min_play_ms)].copy()

    # 期間
    if "ts" in df.columns and (year_start or year_end):
        df = df[df["ts"].notna()].copy()
        if year_start:
            df = df[df["ts"].dt.year >= int(year_start)]
        if year_end:
            df = df[df["ts"].dt.year <= int(year_end)]

    # 列整形
    df = df.rename(
        columns={
            "master_metadata_track_name": "track",
            "master_metadata_album_artist_name": "artist",
            "ms_played": "ms",
            "spotify_track_uri": "uri",
        }
    )

    df = df[["ts", "ms", "artist", "track", "uri"]]
    print(f"[INFO] 有効データ件数: {len(df)}")
    return df


# ---------------------------
# 3) 集計と並び順の確定
# ---------------------------
def _aggregate_and_order(
    df: pd.DataFrame,
    top_artists: int,
    tracks_per_artist: int,
    min_track_total_ms: Optional[int],
) -> Tuple[pd.DataFrame, List[str]]:
    print("[STEP 3/6] 集計・並べ替え中...")

    # アーティスト合計
    artist_totals = (
        df.groupby("artist", dropna=False)["ms"]
        .sum()
        .sort_values(ascending=False)
        .reset_index(name="artist_ms")
    )
    artist_top = artist_totals.head(int(top_artists))
    print(f"[INFO] 上位アーティスト採用: {len(artist_top)}")

    # 曲合計
    track_totals = (
        df.groupby(["artist", "track", "uri"], dropna=False)["ms"]
        .sum()
        .reset_index(name="track_ms")
    )

    # 曲総再生のしきい値
    if min_track_total_ms is not None:
        before = len(track_totals)
        track_totals = track_totals[track_totals["track_ms"] > int(
            min_track_total_ms)].copy()
        print(
            f"[INFO] 曲総再生フィルタ: {before} → {len(track_totals)} 件（> {int(min_track_total_ms)} ms）")

    # 対象アーティストのみ
    track_totals = track_totals.merge(
        artist_top[["artist", "artist_ms"]], on="artist", how="inner")

    # 並べ替え（アーティスト降順 → 曲降順）
    track_totals = track_totals.sort_values(
        by=["artist_ms", "track_ms"], ascending=[False, False], kind="mergesort"
    )

    # アーティスト内ランク
    track_totals["rank_in_artist"] = track_totals.groupby("artist")["track_ms"].rank(
        method="first", ascending=False
    )
    ordered = track_totals[track_totals["rank_in_artist"]
                           <= int(tracks_per_artist)].copy()

    # 最終順（アーティスト降順 → ランク昇順）
    ordered = ordered.sort_values(
        by=["artist_ms", "rank_in_artist"], ascending=[False, True], kind="mergesort"
    ).reset_index(drop=True)

    # URI重複除去（順序保持）
    uris: List[str] = []
    seen = set()
    for u in ordered["uri"]:
        if pd.isna(u):
            continue
        if u not in seen:
            seen.add(u)
            uris.append(u)

    print(f"[INFO] 最終曲数: {len(uris)}")
    return ordered, uris


# ---------------------------
# 4) CSV出力
# ---------------------------
def _sanitize_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "playlist"


def _write_csv(ordered_df: pd.DataFrame, playlist_name: str, out_dir: str = "csv") -> str:
    print("[STEP 4/6] CSV出力中...")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    fname = f"playlist_{_sanitize_filename(playlist_name)}_{ts}.csv"
    path = os.path.join(out_dir, fname)

    ordered_df_out = ordered_df[["artist", "track",
                                 "uri", "track_ms", "artist_ms"]].copy()
    ordered_df_out.to_csv(
        path, index=False, encoding="utf-8-sig", lineterminator="\r\n")

    print(f"[INFO] CSV出力完了: {path}")
    return path


# ---------------------------
# 5) Spotifyプレイリスト作成・曲追加
# ---------------------------
def _create_playlist_and_add(sp: Spotify, user_id: str, name: str, uris: List[str], description: str) -> str:
    print("[STEP 5/6] Spotifyにプレイリストを作成しています...")
    playlist = sp.user_playlist_create(
        user=user_id,
        name=name,
        description=description[:300],  # Spotify上限
    )
    playlist_id = playlist["id"]

    total = len(uris)
    for i in range(0, total, 100):
        chunk = uris[i: i + 100]
        sp.playlist_add_items(playlist_id, chunk)
        print(f"[INFO] 曲を追加中... {i + len(chunk)}/{total}")
        time.sleep(0.2)

    print(f"[INFO] 曲追加完了: {total} 曲")
    return playlist.get("external_urls", {}).get("spotify", "")


# ---------------------------
# 6) メインAPI
# ---------------------------
def create_playlist(
    playlist_name: str = "My Most Played Artists",
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    top_artists: int = 30,
    tracks_per_artist: int = 5,
    min_play_ms: int = 30000,
    source_dir: str = "./source_data",
    min_track_total_ms: Optional[int] = None,
    artists_filter_file: Optional[str] = None,
    artists_filter_mode: str = "exclude",  # 'exclude' / 'include'
) -> None:
    """プレイリスト生成のフルパイプライン。例外は上位（CLI）で処理。"""
    print("===============================================")
    print("🎵 Spotify My Most Played Artists Playlist Generator 実行開始")
    print("===============================================")

    # 説明文用
    command_str = " ".join(sys.argv)
    description = (
        "Generated by Spotify My Most Played Artists Playlist Generator | "
        f"Command: {command_str}"
    )

    # 0) パターン読み込み
    patterns = _load_artist_patterns(artists_filter_file)

    # 1) データ読み込み
    df_raw = _load_history(source_dir)

    # 2) フィルタリング
    df = _filter_music_rows(df_raw, min_play_ms=min_play_ms,
                            year_start=year_start, year_end=year_end)
    if df.empty:
        raise RuntimeError("No valid music rows after filtering.")

    # 2.5) アーティストパターン適用
    if patterns:
        df = _apply_artist_patterns(df, patterns, artists_filter_mode)

    # 3) 集計・順序決定
    ordered_df, uris = _aggregate_and_order(
        df,
        top_artists=top_artists,
        tracks_per_artist=tracks_per_artist,
        min_track_total_ms=min_track_total_ms,
    )
    if not uris:
        raise RuntimeError("No valid Spotify track URIs to add.")

    # 4) CSV出力
    csv_path = _write_csv(ordered_df, playlist_name, out_dir="csv")

    # 5) Spotify 認証
    print("[STEP 6/6] Spotify認証中...")
    sp = Spotify(auth_manager=SpotifyOAuth(scope="playlist-modify-public"))
    me = sp.current_user()
    user_id = me["id"]
    print(f"[AUTH OK] ユーザー: {me.get('display_name') or user_id} ({user_id})")

    # 6) プレイリスト作成・曲追加
    url = _create_playlist_and_add(
        sp, user_id, playlist_name, uris, description=description)

    print("===============================================")
    print(f"[DONE] プレイリスト作成完了: {playlist_name}")
    if url:
        print(f"[URL] {url}")
    print(f"[CSV] 出力: {csv_path}")
    print("===============================================")

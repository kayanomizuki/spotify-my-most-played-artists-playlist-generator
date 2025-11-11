# cli.py
# CLIエントリポイント。generator.create_playlist を実行します。

from __future__ import annotations

import argparse
import sys
from typing import Optional

from dotenv import load_dotenv

from generator import create_playlist


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="Spotify My Most Played Artists Playlist Generator",
        description=(
            "Spotifyの長期ストリーミング履歴(JSON)から、"
            "「よく聴くアーティスト順 → 各アーティスト内でよく聴く曲順」のプレイリストを自動生成し、"
            "同一順序のCSVも出力します。"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--playlist_name", type=str, default="My Most Played Artists",
                        help="作成するプレイリスト名（同名は新規作成）")
    parser.add_argument("--year_start", type=int,
                        default=None, help="対象開始年（例: 2023）")
    parser.add_argument("--year_end", type=int,
                        default=None, help="対象終了年（例: 2025）")
    parser.add_argument("--top_artists", type=int,
                        default=30, help="上位アーティストの採用数")
    parser.add_argument("--tracks_per_artist", type=int,
                        default=5, help="各アーティストから採用する上位曲数")
    parser.add_argument("--min_play_ms", type=int,
                        default=30000, help="単一再生の最小再生時間（ms）")
    parser.add_argument("--min_track_total_ms", type=int, default=None,
                        help="曲単位の総再生時間しきい値（ms）。N ms 以下を除外（>Nのみ採用）")

    # アーティストパターン: exclude / include
    parser.add_argument("--artists_filter_file", type=str, default=None,
                        help=("UTF-8テキスト。1行1パターンの部分一致。空行/先頭#は無視。"
                              "例: 'beatles' を含むアーティスト"))
    parser.add_argument("--artists_filter_mode", type=str, default="exclude",
                        choices=["exclude", "include"],
                        help="パターンに一致したアーティストの扱い。exclude=除外 / include=“のみ”採用")

    parser.add_argument("--source_dir", type=str, default="./source_data",
                        help="Spotify長期ストリーミング履歴JSONの配置ディレクトリ")

    return parser.parse_args()


def _validate_years(start: Optional[int], end: Optional[int]) -> None:
    if start is not None and end is not None and start > end:
        raise ValueError(
            f"Invalid year range: year_start({start}) must be <= year_end({end}).")


def _validate_non_negative(name: str, value: Optional[int]) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{name} must be >= 0 (got {value}).")


def main() -> int:
    load_dotenv()  # SPOTIPY_CLIENT_* / SPOTIPY_REDIRECT_URI を読み込み

    args = parse_args()

    try:
        _validate_years(args.year_start, args.year_end)
        _validate_non_negative("min_play_ms", args.min_play_ms)
        _validate_non_negative("min_track_total_ms", args.min_track_total_ms)

        create_playlist(
            playlist_name=args.playlist_name,
            year_start=args.year_start,
            year_end=args.year_end,
            top_artists=args.top_artists,
            tracks_per_artist=args.tracks_per_artist,
            min_play_ms=args.min_play_ms,
            source_dir=args.source_dir,
            min_track_total_ms=args.min_track_total_ms,
            artists_filter_file=args.artists_filter_file,
            artists_filter_mode=args.artists_filter_mode,
        )
        return 0

    except KeyboardInterrupt:
        print("\n[ABORTED] ユーザーにより中断されました。")
        return 130

    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

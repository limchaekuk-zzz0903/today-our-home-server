# 학습 페이지(숫자섬 수학 탐험대) 앱 연동 가이드

오늘우리집 회원이 앱 안에서 초등 1~6학년 수학 학습 페이지를 쓸 수 있도록 서버에 연동해 두었습니다.
앱에서는 **버튼 하나와 WebView 화면 하나**만 추가하면 됩니다.

## 동작 방식

```
[앱] 학습 버튼 탭
  └─ POST /api/learning/session   (평소처럼 X-Device-ID, X-Device-Secret 헤더)
       ← { "url": "https://<서버>/learn/?ticket=...", "expires_in": 300 }
[앱] url 을 WebView 로 열기
  └─ 서버: 입장권 확인 → 로그인 쿠키(30일) 설정 → /learn/ 학습 페이지 표시
[학습 페이지] 별·크리스탈 진도를 서버에 자동 저장 (/learn/api/progress)
[앱] GET /api/learning/family-progress 로 가족 구성원별 학습 현황 표시 (선택)
```

- 입장권은 **5분 안에 한 번만** 쓸 수 있습니다. 버튼을 누를 때마다 새로 받으세요.
- 앱을 거치지 않고 `/learn/` 을 열면 "오늘우리집 앱에서 열어 주세요" 안내만 보입니다.
- 기기가 로그아웃되면(device secret 삭제) 학습 세션도 함께 무효가 됩니다.
- 진도는 기기(회원) 단위로 저장되고, 여러 번 저장돼도 별은 가장 높은 값이 유지됩니다.

## API

| 메서드 | 경로 | 인증 | 설명 |
|---|---|---|---|
| POST | `/api/learning/session?start=frac-1` | 기기 헤더 | 학습 페이지 1회용 URL 발급. `start`(선택)로 시작할 화 지정 |
| GET | `/api/learning/progress` | 기기 헤더 | 내 진도 요약 `{lessons_done, stars_total, crystals}` |
| GET | `/api/learning/family-progress` | 기기 헤더 | 가족 구성원별 진도 요약 (`total_lessons: 42`, `total_crystals: 13`) |
| GET | `/learn/` | 쿠키 | 학습 페이지 (WebView 전용) |
| GET/PUT | `/learn/api/progress` | 쿠키 | 학습 페이지가 내부적으로 사용 |

`start` 값:
- 학년 지도: `g1`~`g6` (예: 자녀 학년에 맞춰 `?start=g4`)
- 1학년: `seed-1`~`seed-3`, `hill-1`~`hill-3`
- 2학년: `river-1`~`river-3`, `gugu-1`~`gugu-3`
- 3학년: `mul-1`~`mul-3`, `div-1`~`div-3`, `frac-1`~`frac-6`
- 4학년: `port-1`~`port-3`, `light-1`~`light-3`
- 5학년: `jungle-1`~`jungle-3`, `volcano-1`~`volcano-3`
- 6학년: `canyon-1`~`canyon-3`, `observ-1`~`observ-3`
- 대장 문제: `<지역>-boss` (예: `port-boss`), 처음 화면: `home`

`family-progress` 응답 예:

```json
{
  "total_lessons": 42,
  "total_crystals": 13,
  "members": [
    {"device_id": "…", "user_name": "회은", "updated_at": "2026-11-08T02:59:16",
     "lessons_done": 3, "stars_total": 7, "crystals": ["mul"]}
  ]
}
```

## 앱 코드 예시

### Flutter (`webview_flutter`)

```dart
Future<void> openLearning(BuildContext context, {String? start}) async {
  final res = await http.post(
    Uri.parse('$baseUrl/api/learning/session${start != null ? '?start=$start' : ''}'),
    headers: {'X-Device-ID': deviceId, 'X-Device-Secret': deviceSecret},
  );
  final url = jsonDecode(res.body)['url'] as String;
  final controller = WebViewController()
    ..setJavaScriptMode(JavaScriptMode.unrestricted)
    ..loadRequest(Uri.parse(url));
  Navigator.of(context).push(MaterialPageRoute(
    builder: (_) => Scaffold(
      appBar: AppBar(title: const Text('수학 탐험대')),
      body: SafeArea(child: WebViewWidget(controller: controller)),
    ),
  ));
}
```

### React Native (`react-native-webview`)

```tsx
const [url, setUrl] = useState<string | null>(null);
useEffect(() => {
  fetch(`${BASE_URL}/api/learning/session`, {
    method: 'POST',
    headers: { 'X-Device-ID': deviceId, 'X-Device-Secret': deviceSecret },
  }).then(r => r.json()).then(j => setUrl(j.url));
}, []);
return url ? <WebView source={{ uri: url }} javaScriptEnabled domStorageEnabled sharedCookiesEnabled /> : <ActivityIndicator />;
```

WebView 설정 체크리스트: JavaScript 켜기, DOM storage(localStorage) 켜기, 쿠키 허용(Android는 기본 허용, iOS는 `sharedCookiesEnabled` 권장).

## 학습 페이지 갱신

학습 페이지 원본은 `limchaekuk-zzz0903/servicemap` 저장소의 `math-adventure/` 입니다. 내용을 바꾼 뒤 이 저장소에서:

```bash
scripts/sync_learn.sh ../servicemap   # 경로를 생략하면 GitHub main 에서 내려받음
git add learn_static && git commit -m "Update learning page"
```

## 참고

- 학습 페이지의 "셈이에게 물어보기"는 오늘우리집 서버에서는 **기본 도움말 모드**(낱말 기반 내장 설명)로 동작합니다.
- 테이블 `learning_tickets`, `learning_sessions`, `learning_progress` 는 서버 시작 시 자동 생성됩니다(마이그레이션 불필요).

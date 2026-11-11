# 학습 페이지(지혜섬 탐험대) 앱 연동 가이드

오늘우리집 앱 안에서 초등 학습 페이지 **지혜섬 탐험대**를 쓸 수 있도록 서버에 연동해 두었습니다.

> **학습 페이지에는 회원·로그인 개념이 없습니다.** `/learn/`은 누구나 열 수 있습니다.
> 오늘우리집 연동은 "회원이면 진도를 서버(계정)에 저장해 주는 것"뿐이고, 페이지를 막는 데 쓰지 않습니다.
> 입장권이 없거나, 이미 썼거나, 만료돼도 페이지는 그대로 열리고 진도는 기기 안에 저장됩니다.
> 이 규칙은 `tests/test_learning.py`가 지키며, GitHub Actions(`Tests`)에서 push·PR마다 확인합니다.
처음에는 수학만 있었지만 지금은 **수학·국어·영어·과학 4과목**이 있습니다.

| 과목 | 학년 | 화 | 크리스탈(대장 문제) |
|---|---|---|---|
| 🧮 수학 | 1~6학년 | 49 | 13 |
| 📖 국어 | 1~3학년 | 9 | 3 |
| 🔤 영어 | 3~5학년 | 9 | 3 |
| 🔬 과학 | 3~5학년 | 9 | 3 |
| 합계 | | 76 | 22 |

과목·학년·화 구성은 서버의 `learn_static/catalog.json`에 있고, 앱은 `GET /api/learning/subjects`로 받아 씁니다.
**과목이나 화가 늘어나도 앱을 다시 빌드할 필요가 없도록** 과목 이름, 개수, 학년을 앱에 고정하지 말고 이 API에서 받아 그려 주세요.

## 앱에서 바꿀 것 (수학 전용 → 여러 과목)

| 예전(수학 전용) | 지금 |
|---|---|
| 메뉴·버튼 이름 "수학 탐험대", "수학 공부" | **"지혜섬 탐험대"** 또는 **"학습"** |
| 버튼 하나 → 수학 페이지 | **과목 선택 화면**(과목 카드 4개) → 과목 페이지. 버튼 하나만 둘 때는 `start` 없이 열면 학습 페이지의 과목 선택 첫 화면이 나옵니다 |
| 진도 카드: 별·크리스탈 합계 | **과목별 진도**(푼 화/전체, 크리스탈)와 레벨·경험치·배지 |
| 가족 현황: 총 76화 중 몇 화 | 구성원마다 **과목별** 진도 막대 |
| `start=g4` (수학 학년) | `start=kor`(과목 페이지), `start=eng-g4`(과목·학년), `start=daily`(오늘의 도전) 등 |

## 동작 방식

```
[앱] 학습 메뉴 진입
  └─ GET /api/learning/subjects          → 과목 카드 4개 (이름·아이콘·학년 범위·화 수)
  └─ GET /api/learning/progress          → 과목별 내 진도 (선택)
[앱] 과목 카드 탭 (예: 국어)
  └─ POST /api/learning/session?start=kor   (평소처럼 X-Device-ID, X-Device-Secret 헤더)
       ← { "url": "https://<서버>/learn/?ticket=...", "public_url": "https://<서버>/learn/#kor", "expires_in": 300 }
[앱] url 을 WebView 로 열기 (발급이 실패하면 public_url 또는 https://<서버>/learn/ 을 그대로 열기)
  └─ 서버: 입장권이 유효하면 진도 저장용 쿠키(30일) 설정 → /learn/#kor (국어 과목 페이지)
           입장권이 없거나 무효여도 → /learn/#kor 로 열림 (진도는 기기 안에만 저장)
[학습 페이지] 별·크리스탈·경험치·배지·메달을 서버에 자동 저장 (/learn/api/progress)
[앱] GET /api/learning/family-progress → 가족 구성원별·과목별 학습 현황 (선택)
```

- 입장권은 **5분 안에 한 번만** 쓸 수 있습니다. 버튼을 누를 때마다 새로 받으세요.
- 앱을 거치지 않고 `/learn/`을 열어도 학습 페이지가 그대로 열립니다. 이때 진도는 그 기기(브라우저)에만 저장됩니다.
- 같은 입장권 주소를 다시 열어도(새로고침, 앱 복귀, WebView 미리 불러오기) 막히지 않습니다. 처음 받은 쿠키로 계속 저장됩니다.
- 응답의 `public_url`은 입장권 없는 주소입니다. 세션 발급이 실패할 때 대신 열면 됩니다(진도는 기기 안에만 저장).
- 기기가 로그아웃되면(device secret 삭제) 학습 세션도 함께 무효가 됩니다.
- 진도는 기기(회원) 단위로 저장됩니다. 여러 번 저장돼도 별·경험치·기록은 큰 값이, 크리스탈·배지·메달은 한 번 얻으면 계속 유지됩니다.
- 오답 노트와 학습 기록(부모님용)은 기기 안(WebView 저장소)에만 남고 서버로 보내지 않습니다.

## API

| 메서드 | 경로 | 인증 | 설명 |
|---|---|---|---|
| GET | `/api/learning/subjects` | 기기 헤더 | 과목 목록(과목 메뉴용) |
| POST | `/api/learning/session?start=kor` | 기기 헤더 | 학습 페이지 1회용 URL 발급. `start`(선택)로 시작 화면 지정 |
| GET | `/api/learning/progress` | 기기 헤더 | 내 진도 요약(과목별 포함) |
| GET | `/api/learning/family-progress` | 기기 헤더 | 가족 구성원별 진도 요약(과목별 포함) |
| GET | `/learn/` | 쿠키 | 학습 페이지 (WebView 전용) |
| GET/PUT | `/learn/api/progress` | 쿠키 | 학습 페이지가 내부적으로 사용 |

### `start` 값

- 생략 또는 `home`: 과목 선택 첫 화면
- 과목 페이지: `math`, `kor`, `eng`, `sci` (과목 카드에 연결할 값은 `subjects[].start`)
- 과목·학년 지도: 수학 `g1`~`g6`, 국어 `kor-g1`~`kor-g3`, 영어 `eng-g3`~`eng-g5`, 과학 `sci-g3`~`sci-g5` (`subjects[].grades[].start`)
- 화: `frac-1`, `hangul-2`, `vocab-1`, `space-3` 처럼 화 id. 대장 문제: `<지역>-boss` (예: `port-boss`)
- `daily`(오늘의 도전), `trophy`(트로피 방), `speed-<과목>-g<학년>`(60초 스피드 도전, 예: `speed-eng-g3`)
- 목록에 없는 값은 무시하고 첫 화면을 엽니다.

### `GET /api/learning/subjects` 응답 예

```json
{
  "title": "지혜섬 탐험대",
  "lessons_total": 76,
  "crystals_total": 22,
  "subjects": [
    {"id": "math", "name": "수학", "icon": "🧮", "span": "1~6학년",
     "desc": "수와 연산 중심으로 1학년부터 6학년까지. 학년이 곧 레벨이에요.",
     "start": "math", "lessons_total": 49, "crystals_total": 13,
     "grades": [{"g": 1, "name": "1학년", "island": "새싹 들판", "focus": "수 세기, …",
                 "start": "g1", "regions": ["새싹 마을", "…"]}]},
    {"id": "kor", "name": "국어", "icon": "📖", "span": "1~3학년", "start": "kor", "…": "…"},
    {"id": "eng", "name": "영어", "icon": "🔤", "span": "3~5학년", "start": "eng", "…": "…"},
    {"id": "sci", "name": "과학", "icon": "🔬", "span": "3~5학년", "start": "sci", "…": "…"}
  ]
}
```

### `GET /api/learning/progress` 응답 예

```json
{
  "device_id": "…",
  "lessons_done": 5, "stars_total": 12, "crystals": ["hangul", "mul"],
  "xp": 300, "level": 4, "study_days": 6, "badges": 4, "medals": 1,
  "subjects": {
    "math": {"name": "수학", "lessons_done": 3, "lessons_total": 49, "stars_total": 7, "stars_max": 147,
             "crystals": ["mul"], "crystals_total": 13},
    "kor":  {"name": "국어", "lessons_done": 2, "lessons_total": 9, "stars_total": 5, "stars_max": 27,
             "crystals": ["hangul"], "crystals_total": 3},
    "eng":  {"…": "…"},
    "sci":  {"…": "…"}
  }
}
```

레벨은 학습 페이지와 같은 규칙입니다(레벨 n이 되려면 경험치 20·n·(n+1)). 앱에 "Lv.4" 같은 배지로 보여 주면 아이가 학습 페이지와 같은 숫자를 봅니다.

### `GET /api/learning/family-progress` 응답 예

```json
{
  "total_lessons": 76,
  "total_crystals": 22,
  "subjects": [{"id": "math", "name": "수학", "icon": "🧮", "lessons_total": 49, "crystals_total": 13}, "…"],
  "members": [
    {"device_id": "…", "user_name": "회은", "updated_at": "2026-11-10T02:59:16",
     "lessons_done": 5, "stars_total": 12, "crystals": ["hangul", "mul"],
     "xp": 300, "level": 4, "study_days": 6, "badges": 4, "medals": 1,
     "subjects": {"math": {"lessons_done": 3, "lessons_total": 49, "…": "…"}, "kor": {"…": "…"}}}
  ]
}
```

## 앱 코드 예시

### Flutter (`webview_flutter`)

```dart
/// 학습 메뉴: 과목 카드 목록
class LearningHome extends StatefulWidget {
  const LearningHome({super.key});
  @override
  State<LearningHome> createState() => _LearningHomeState();
}

class _LearningHomeState extends State<LearningHome> {
  List<dynamic> subjects = [];
  Map<String, dynamic> mine = {};

  Map<String, String> get _headers => {'X-Device-ID': deviceId, 'X-Device-Secret': deviceSecret};

  @override
  void initState() {
    super.initState();
    () async {
      final s = await http.get(Uri.parse('$baseUrl/api/learning/subjects'), headers: _headers);
      final p = await http.get(Uri.parse('$baseUrl/api/learning/progress'), headers: _headers);
      setState(() {
        subjects = jsonDecode(s.body)['subjects'];
        mine = jsonDecode(p.body);
      });
    }();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('지혜섬 탐험대${mine['level'] != null ? ' · Lv.${mine['level']}' : ''}')),
      body: ListView(children: [
        for (final s in subjects)
          Builder(builder: (_) {
            final p = (mine['subjects'] ?? {})[s['id']] ?? {};
            final done = p['lessons_done'] ?? 0, total = s['lessons_total'];
            return ListTile(
              leading: Text(s['icon'], style: const TextStyle(fontSize: 32)),
              title: Text('${s['name']} · ${s['span']}'),
              subtitle: LinearProgressIndicator(value: total > 0 ? done / total : 0),
              trailing: Text('$done/$total화'),
              onTap: () => openLearning(context, start: s['start']),
            );
          }),
        ListTile(
          leading: const Text('🎯', style: TextStyle(fontSize: 32)),
          title: const Text('오늘의 도전'),
          onTap: () => openLearning(context, start: 'daily'),
        ),
      ]),
    );
  }
}

Future<void> openLearning(BuildContext context, {String? start}) async {
  final res = await http.post(
    Uri.parse('$baseUrl/api/learning/session${start != null ? '?start=$start' : ''}'),
    headers: {'X-Device-ID': deviceId, 'X-Device-Secret': deviceSecret},
  );
  // 발급이 실패해도(로그아웃, 네트워크 오류 등) 학습 페이지는 열어 준다. 이때 진도는 기기 안에만 저장된다.
  final url = res.statusCode == 200
      ? jsonDecode(res.body)['url'] as String
      : '$baseUrl/learn/${start != null ? '#$start' : ''}';
  final controller = WebViewController()
    ..setJavaScriptMode(JavaScriptMode.unrestricted)
    ..loadRequest(Uri.parse(url));
  if (!context.mounted) return;
  Navigator.of(context).push(MaterialPageRoute(
    builder: (_) => Scaffold(
      appBar: AppBar(title: const Text('지혜섬 탐험대')),
      body: SafeArea(child: WebViewWidget(controller: controller)),
    ),
  ));
}
```

### React Native (`react-native-webview`)

```tsx
const H = { 'X-Device-ID': deviceId, 'X-Device-Secret': deviceSecret };

export function LearningHome({ navigation }) {
  const [subjects, setSubjects] = useState([]);
  const [mine, setMine] = useState<any>({});
  useEffect(() => {
    fetch(`${BASE_URL}/api/learning/subjects`, { headers: H }).then(r => r.json()).then(j => setSubjects(j.subjects));
    fetch(`${BASE_URL}/api/learning/progress`, { headers: H }).then(r => r.json()).then(setMine);
  }, []);
  return (
    <ScrollView>
      <Text>지혜섬 탐험대 {mine.level ? `Lv.${mine.level}` : ''}</Text>
      {subjects.map(s => {
        const p = mine.subjects?.[s.id] ?? { lessons_done: 0 };
        return (
          <Pressable key={s.id} onPress={() => navigation.navigate('Learning', { start: s.start })}>
            <Text>{s.icon} {s.name} · {s.span}  {p.lessons_done}/{s.lessons_total}화</Text>
          </Pressable>
        );
      })}
      <Pressable onPress={() => navigation.navigate('Learning', { start: 'daily' })}><Text>🎯 오늘의 도전</Text></Pressable>
    </ScrollView>
  );
}

export function Learning({ route }) {
  const [url, setUrl] = useState<string | null>(null);
  useEffect(() => {
    const q = route.params?.start ? `?start=${route.params.start}` : '';
    fetch(`${BASE_URL}/api/learning/session${q}`, { method: 'POST', headers: H })
      .then(r => (r.ok ? r.json() : Promise.reject()))
      .then(j => setUrl(j.url))
      // 발급이 실패해도 학습 페이지는 연다(진도는 기기 안에만 저장)
      .catch(() => setUrl(`${BASE_URL}/learn/${route.params?.start ? '#' + route.params.start : ''}`));
  }, [route.params?.start]);
  return url ? <WebView source={{ uri: url }} javaScriptEnabled domStorageEnabled sharedCookiesEnabled mediaPlaybackRequiresUserAction={false} /> : <ActivityIndicator />;
}
```

WebView 설정 체크리스트

- JavaScript 켜기, DOM storage(localStorage) 켜기, 쿠키 허용(Android는 기본 허용, iOS는 `sharedCookiesEnabled` 권장)
- **소리**: 앱을 고치지 않아도 서버 읽어 주기 음성으로 소리가 납니다(아래 "소리(읽어 주기)"). WebView에서 오디오 재생을 막지 않았는지만 확인해 주세요(Flutter `webview_flutter`는 기본값 그대로면 됩니다, RN은 `mediaPlaybackRequiresUserAction={false}` 권장).
- **키보드**: 국어·영어 도전 단계는 직접 쓰는 문제가 많습니다. WebView 화면에 `resizeToAvoidBottomInset`(Flutter) / `KeyboardAvoidingView`(RN)를 적용해 입력 칸이 키보드에 가리지 않게 해 주세요.

## 소리(읽어 주기)

학습 페이지는 만화 읽어 주기, 국어 받아쓰기, 영어 듣기에 소리를 씁니다.
**Android 앱 화면(WebView)에는 웹 음성 합성(Web Speech)이 없어서**, 브라우저 기능만으로는 소리가 나지 않습니다.
그래서 소리를 내는 방법이 세 가지이고, 학습 페이지가 아래 순서로 고릅니다.

1. **앱 음성 연결(브리지)**: 앱이 연결해 두면 휴대폰 TTS로 읽습니다. (선택, 아래 코드)
2. **서버 읽어 주기 음성 (기본, 앱 수정 불필요)**: 오늘우리집 서버에서 열린 학습 페이지(`/learn/`)는 Android 앱 화면이거나
   브라우저 음성이 없거나 실패하면 `GET /learn/api/tts?t=<글>&l=ko-KR|en-US`로 mp3를 받아 재생합니다.
3. **브라우저 음성**: 일반 브라우저(크롬, 사파리 등)에서는 기기 음성으로 바로 읽습니다.

셋 다 안 되면 "소리가 나오지 않아요" 안내가 뜨고, 듣기 문제에 "부모님이 읽어 주기" 버튼이 나타납니다.

### 서버 읽어 주기 음성 설정 (환경 변수)

| 변수 | 값 | 설명 |
|---|---|---|
| `TTS_ENGINE` | `edge` (기본) | Microsoft Edge 읽어 주기 음성(한국어 SunHi, 영어 Ana). 키가 필요 없지만 공식 API가 아니에요. **edge-tts 7.3.1 이상 필요**(2026-11 Microsoft 규칙 변경으로 7.2.7 이하는 403) |
| | `gtranslate` | Google 번역 읽기 음성. 키 불필요 |
| | `google` | Google Cloud Text-to-Speech(공식 API). `GOOGLE_TTS_API_KEY`가 필요하고 사용량만큼 요금이 나와요 |
| | `off` | 끔 |
| `TTS_FALLBACK` | `gtranslate` (기본) | 기본 엔진이 실패하면 쓰는 예비 엔진. `none`이면 예비 없음 |
| `GOOGLE_TTS_API_KEY` | API 키 | `google` 엔진일 때만 |
| `TTS_CACHE_DIR` | 경로 | 만든 음성을 저장하는 곳(기본 `.tts_cache/`). 같은 글은 다시 만들지 않아요 |

- 만화 대사처럼 같은 글은 한 번 만든 뒤 캐시에서 바로 나갑니다(브라우저도 1년 동안 캐시).
- 남용 방지: 글 400자 이하, 한국어·영어만, IP당 10분에 새 음성 240개까지(캐시된 음성은 제한 없음).
- `GET /learn/api/tts/status`: 켜짐 여부, 마지막 성공·실패 원인(`last_error`), 실제 쓴 엔진(`engine_used`)
- `GET /learn/api/tts/selftest`: 지금 음성을 새로 만들어 보고 결과를 알려 줍니다. **소리가 안 날 때 브라우저로 `https://<서버>/learn/api/tts/selftest`를 열어 보세요.**
- GitHub Actions `Diagnose read-aloud`: 엔진이 데이터센터에서 동작하는지 점검(저장소 변수 `LEARN_BASE_URL`에 서버 주소를 넣으면 운영 서버도 점검)
- 서버가 바깥 인터넷(엔진)에 연결할 수 있어야 합니다. 연결이 안 되면 503을 주고, 학습 페이지는 "부모님이 읽어 주기"로 넘어갑니다.

### 아이폰 앱: 무음 모드에서도 소리가 나게 (권장)

아이폰은 앱의 오디오 설정(AVAudioSession)이 기본값이면, 무음 스위치를 켰을 때 앱 안 웹 화면의 소리까지 꺼질 수 있습니다.
학습 페이지는 iOS 17 이상에서 스스로 "미디어 재생"으로 요청하지만(`navigator.audioSession`), 확실하게 하려면 앱에서 한 줄을 넣어 주세요.

Swift (`AppDelegate.swift`의 `application(_:didFinishLaunchingWithOptions:)`):

```swift
import AVFoundation
try? AVAudioSession.sharedInstance().setCategory(.playback, mode: .spokenAudio, options: [.mixWithOthers])
```

Flutter (`audio_session` 패키지):

```dart
final session = await AudioSession.instance;
await session.configure(const AudioSessionConfiguration.speech().copyWith(
  avAudioSessionCategory: AVAudioSessionCategory.playback,
));
```

React Native: `react-native-sound`의 `Sound.setCategory('Playback', true)` 또는 Expo `Audio.setAudioModeAsync({ playsInSilentModeIOS: true })`.

### (선택) 앱 음성 연결

앱에서 휴대폰 TTS로 읽게 하고 싶으면 아래처럼 연결합니다. 연결하면 서버 음성보다 먼저 쓰입니다(인터넷 없이도 동작).

> ⚠️ 학습 페이지는 앱이 **명시적으로 알릴 때만** 앱 TTS를 씁니다: Flutter 채널 이름 `TodayTTS`, iOS 메시지 핸들러 `TodayTTS`, 또는 페이지에 `window.TodayTTSEnabled = true`를 넣은 경우(React Native는 `injectedJavaScriptBeforeContentLoaded="window.TodayTTSEnabled=true;"`). React Native·flutter_inappwebview가 기본으로 만드는 통로만으로는 쓰지 않습니다(앱이 처리하지 않으면 소리가 사라지기 때문).

학습 페이지가 보내는 메시지 (JSON)

```json
{"type": "speak", "text": "운동장", "lang": "ko-KR"}   // lang: ko-KR 또는 en-US
{"type": "stop"}
```

### Flutter (`webview_flutter` + `flutter_tts`)

채널 이름은 반드시 `TodayTTS`로 해 주세요.

```dart
final tts = FlutterTts();

final controller = WebViewController()
  ..setJavaScriptMode(JavaScriptMode.unrestricted)
  ..addJavaScriptChannel('TodayTTS', onMessageReceived: (msg) async {
    final m = jsonDecode(msg.message) as Map<String, dynamic>;
    if (m['type'] == 'stop') { await tts.stop(); return; }
    if (m['type'] == 'speak') {
      final lang = m['lang'] as String? ?? 'ko-KR';
      await tts.stop();
      await tts.setLanguage(lang);
      await tts.setSpeechRate(lang.startsWith('en') ? 0.42 : 0.47); // 아이가 알아듣기 쉬운 속도
      await tts.speak(m['text'] as String);
    }
  })
  ..loadRequest(Uri.parse(url));
```

`flutter_inappwebview`를 쓰면 `addJavaScriptHandler(handlerName: 'tts', callback: (args) { final m = args[0]; ... })`로 같은 메시지를 받습니다.

### React Native (`react-native-webview` + `expo-speech` 또는 `react-native-tts`)

```tsx
import * as Speech from 'expo-speech';

<WebView
  source={{ uri: url }}
  javaScriptEnabled domStorageEnabled sharedCookiesEnabled
  onMessage={e => {
    let m: any; try { m = JSON.parse(e.nativeEvent.data); } catch { return; }
    if (m.source !== 'learn') return;
    if (m.type === 'stop') { Speech.stop(); return; }
    if (m.type === 'speak') {
      Speech.stop();
      Speech.speak(m.text, { language: m.lang, rate: m.lang?.startsWith('en') ? 0.85 : 0.92 });
    }
  }}
/>
```

### 확인 방법

1. 앱에서 학습 → 국어 → 1학년 → "듣고 쓰기" 화를 열고 "소리 듣기"를 누르면 낱말을 읽어 줍니다.
2. 영어 → 3학년 → "첫소리 듣고 글자 찾기"에서 영어 발음이 나오는지 확인합니다.
3. 소리가 안 나오면 기기 설정 → 접근성(또는 일반) → 텍스트 음성 변환(TTS)에서 한국어·영어 음성 데이터를 내려받았는지 확인합니다.

## 학습 페이지 갱신

학습 페이지 원본은 `limchaekuk-zzz0903/Elementaryschoolworksheets` 저장소입니다. 내용을 바꾼 뒤 이 저장소에서:

```bash
scripts/sync_learn.sh ../elementaryschoolworksheets   # 경로를 생략하면 GitHub main 에서 내려받음
git add learn_static && git commit -m "Update learning page"
```

`catalog.json`도 함께 복사되므로, 과목·화가 늘어나면 서버를 다시 시작하는 것만으로 과목 목록과 진도 집계가 바뀝니다(앱 재빌드 불필요).

## 참고

- 학습 페이지의 "셈이에게 물어보기"는 오늘우리집 서버에서는 **기본 도움말 모드**(낱말 기반 내장 설명)로 동작합니다.
- 테이블 `learning_tickets`, `learning_sessions`, `learning_progress`는 서버 시작 시 자동 생성됩니다(마이그레이션 불필요). 저장 형식이 늘어났지만 예전 진도도 그대로 읽습니다.

tell application "iTerm"
    create window with default profile
    tell current session of current window
        write text "cd ~/go/src/go-hermes"
        write text "go run server/main.go -config config.json -id 1.1"
        my makeTab()
    end tell
    tell current session of current window
        write text "cd ~/go/src/go-hermes"
        write text "go run server/main.go -config config.json -id 1.2"
        my makeTab()
    end tell
    tell current session of current window
        write text "cd ~/go/src/go-hermes"
        write text "go run server/main.go -config config.json -id 1.3"
        my makeTab()
    end tell
    tell current session of current window
        write text "cd ~/go/src/go-hermes"
        write text "go run server/main.go -config config.json -id 1.4"
        my makeTab()
    end tell
    tell current session of current window
        write text "cd ~/go/src/go-hermes"
        write text "go run server/main.go -config config.json -id 1.5"
        my makeTab()
    end tell
    tell current session of current window
        write text "cd ~/go/src/go-hermes"
        write text "go run server/main.go -config config.json -id 1.6"
        my makeTab()
    end tell
--    tell current session of current window
--        write text "cd ~/go/src/go-hermes"
--        write text "go run server/main.go -config config.json -id 3.1"
--        my makeTab()
--    end tell
--    tell current session of current window
--        write text "cd ~/go/src/go-hermes"
--        write text "go run server/main.go -config config.json -id 3.2"
--        my makeTab()
--    end tell
--    tell current session of current window
--        write text "cd ~/go/src/go-hermes"
--        write text "go run server/main.go -config config.json -id 3.3"
--    end tell
end tell

on makeTab()
    tell application "System Events" to keystroke "t" using {command down}
    delay 0.2
end makeTab
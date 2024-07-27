def clean_phase(phase_text):
    phase_text = str(phase_text)

    if 'Phase III ' in phase_text:
        return 'Phase 3'
    elif 'Phase II ' in phase_text:
        return 'Phase 2'
    elif 'Phase I ' in phase_text:
        return 'Phase 1'
    elif phase_text in ["Phase 1", "Phase I", "1", "phase1", "I"]:
        return "Phase 1"
    elif phase_text in ["Phase 2", "Phase II", "2", "phase2", "II"]:
        return "Phase 2"
    elif phase_text in ["Phase 3", "Phase III", "3", "phase3", "III"]:
        return "Phase 3"
    elif phase_text in ["Registration", "R"]:
        return "Registration"
    else:
        return str(phase_text.title())
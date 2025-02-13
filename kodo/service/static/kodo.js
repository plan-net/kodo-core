function getTheme(localStorageTheme, systemSettingDark) {
    if (localStorageTheme) {
        console.log("return local storage theme: ", localStorageTheme);
        return localStorageTheme;
    }
    const systemTheme = systemSettingDark.matches ? "dark" : "light";
    return systemTheme;
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem("theme", theme);
}

const localStorageTheme = localStorage.getItem("theme");
const systemSettingDark = window.matchMedia("(prefers-color-scheme: dark)");

let currentThemeSetting = getTheme(localStorageTheme, systemSettingDark);
applyTheme(currentThemeSetting);

systemSettingDark.addEventListener("change", (e) => {
    if (!localStorageTheme) {
        currentThemeSetting = e.matches ? "dark" : "light";
        applyTheme(currentThemeSetting);
    }
});

document.querySelector("[data-theme-toggle]").addEventListener("click", () => {
    currentThemeSetting = currentThemeSetting === "dark" ? "light" : "dark";
    applyTheme(currentThemeSetting);
});
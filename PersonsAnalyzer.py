import os
import math
import sqlite3
import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import ttk
from typing import Dict


def load_data(path: str = "persons.csv") -> pd.DataFrame:
    """Load the person data from a CSV file."""
    df = pd.read_csv(path, low_memory=False)
    return df


def extract_features(df: pd.DataFrame) -> pd.DataFrame:
    """Extract behavioural features from the person DataFrame."""
    features = {}

    # Count the number of individual flight records per person
    flights_count = df["FlightHistory"].fillna("").apply(
        lambda x: len([f for f in str(x).split(",") if f.strip()])
    )
    features["flights_count"] = flights_count

    # Parse departure and arrival city lists
    departure_lists = df["DepartureCities"].fillna("").apply(
        lambda x: [d.strip().upper() for d in str(x).split(",") if d.strip()]
    )
    arrival_lists = df["ArrivalCities"].fillna("").apply(
        lambda x: [a.strip().upper() for a in str(x).split(",") if a.strip()]
    )

    # Number of distinct departure cities
    features["unique_departures_count"] = departure_lists.apply(lambda lst: len(set(lst)))

    # Number of distinct arrival cities
    features["unique_arrivals_count"] = arrival_lists.apply(lambda lst: len(set(lst)))

    def count_repeated_routes(deps: list[str], arrs: list[str]) -> int:
        """Count how many routes appear more than once."""
        route_counts: Dict[tuple[str, str], int] = {}
        for d, a in zip(deps, arrs):
            key = (d, a)
            route_counts[key] = route_counts.get(key, 0) + 1
        return sum(c - 1 for c in route_counts.values() if c > 1)

    repeated_routes = []
    same_city_count = []
    for deps, arrs in zip(departure_lists, arrival_lists):
        repeated_routes.append(count_repeated_routes(deps, arrs))
        same_city_count.append(sum(1 for d, a in zip(deps, arrs) if d == a))

    features["repeated_routes_count"] = pd.Series(repeated_routes)
    features["same_departure_arrival"] = pd.Series(same_city_count)

    feature_df = pd.DataFrame(features)
    return feature_df


def compute_suspicion_scores(features: pd.DataFrame, weights: Dict[str, float]) -> pd.Series:
    """Compute a weighted suspicion score for each person."""
    normalised = pd.DataFrame(index=features.index)
    for col in features.columns:
        col_max = features[col].max()
        if col_max > 0:
            normalised[col] = features[col] / col_max
        else:
            normalised[col] = 0.0

    total_weight = sum(weights.get(col, 0.0) for col in features.columns)
    if total_weight <= 0:
        return pd.Series([0.0] * len(features), index=features.index)

    score = pd.Series(0.0, index=features.index)
    for col in features.columns:
        weight = weights.get(col, 0.0)
        if weight > 0:
            score += weight * normalised[col]
    score = (score / total_weight) * 100
    return score


def assign_group(row: pd.Series, suspicion_thresholds: Dict[str, float], tourist_threshold: int,
                 regular_traveler_threshold: int, business_threshold: int) -> str:
    """Assign a behavioural group label based on score."""
    if row["suspicion_score"] >= suspicion_thresholds.get("high", 70.0):
        return "High Risk"
    if row["flights_count"] >= regular_traveler_threshold and row["unique_departures_count"] <= 2 and row[
        "unique_arrivals_count"] <= 2:
        return "Regular Traveler"
    if row["unique_arrivals_count"] >= tourist_threshold:
        return "Tourist"
    if row["repeated_routes_count"] >= business_threshold:
        return "Frequent Business"
    if row["suspicion_score"] >= suspicion_thresholds.get("medium", 40.0):
        return "Moderate Risk"
    return "Low Risk"


def show_person_details(event, df, feature_df, suspicion_thresholds, tourist_threshold, regular_traveler_threshold,
                        business_threshold):
    """Display full information about the selected person in a new window."""
    selection = listbox.curselection()
    if not selection:
        return

    # Get the selected item and extract PersonID
    selected_text = listbox.get(selection[0])
    person_id = int(selected_text.split(" - ")[0])

    # Get full data for the person
    person_data = df[df["PersonID"] == person_id].iloc[0]

    # Compute features and suspicion score for the person
    current_weights = {feat: slider.get() for feat, slider in weight_sliders.items()}
    scores = compute_suspicion_scores(feature_df, current_weights)
    person_features = feature_df.loc[person_data.name]
    suspicion_score = scores.loc[person_data.name]
    group = assign_group(
        pd.Series({
            **person_features.to_dict(),
            "suspicion_score": suspicion_score
        }),
        suspicion_thresholds,
        tourist_threshold,
        regular_traveler_threshold,
        business_threshold
    )

    # Create a new window for details
    details_window = tk.Toplevel()
    details_window.title(f"Details for PersonID: {person_id}")
    details_window.geometry("600x600")

    # Create a scrollable frame
    canvas = tk.Canvas(details_window)
    scrollbar = ttk.Scrollbar(details_window, orient="vertical", command=canvas.yview)
    scrollable_frame = ttk.Frame(canvas)

    scrollable_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
    )

    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")

    # Display all person details
    ttk.Label(scrollable_frame, text=f"PersonID: {person_data['PersonID']}").pack(anchor="w", padx=5, pady=2)
    ttk.Label(scrollable_frame,
              text=f"Name: {person_data['FirstName']} {person_data.get('MiddleName', '')} {person_data['LastName']}").pack(
        anchor="w", padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Sex: {person_data.get('Sex', 'N/A')}").pack(anchor="w", padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Birth Date: {person_data.get('BirthDate', 'N/A')}").pack(anchor="w", padx=5,
                                                                                               pady=2)
    ttk.Label(scrollable_frame, text=f"Travel Documents: {person_data.get('TravelDocuments', 'N/A')}").pack(anchor="w",
                                                                                                         padx=5,
                                                                                                         pady=2)
    ttk.Label(scrollable_frame, text=f"Loyalty Numbers: {person_data.get('LoyaltyNumbers', 'N/A')}").pack(anchor="w",
                                                                                                       padx=5,
                                                                                                       pady=2)
    ttk.Label(scrollable_frame, text=f"Ticket Numbers: {person_data.get('TicketNumbers', 'N/A')}").pack(anchor="w",
                                                                                                     padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Booking Codes: {person_data.get('BookingCodes', 'N/A')}").pack(anchor="w",
                                                                                                    padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Flight History: {person_data.get('FlightHistory', 'N/A')}").pack(anchor="w",
                                                                                                     padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Departure Cities: {person_data.get('DepartureCities', 'N/A')}").pack(anchor="w",
                                                                                                         padx=5,
                                                                                                         pady=2)
    ttk.Label(scrollable_frame, text=f"Arrival Cities: {person_data.get('ArrivalCities', 'N/A')}").pack(anchor="w",
                                                                                                     padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Loyalty Programs: {person_data.get('LoyaltyPrograms', 'N/A')}").pack(anchor="w",
                                                                                                         padx=5,
                                                                                                         pady=2)
    ttk.Label(scrollable_frame, text=f"Meals: {person_data.get('Meals', 'N/A')}").pack(anchor="w", padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Travel Classes: {person_data.get('TravelClasses', 'N/A')}").pack(anchor="w",
                                                                                                     padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Fare Bases: {person_data.get('FareBases', 'N/A')}").pack(anchor="w", padx=5,
                                                                                               pady=2)
    ttk.Label(scrollable_frame, text=f"Baggages: {person_data.get('Baggages', 'N/A')}").pack(anchor="w", padx=5,
                                                                                           pady=2)
    ttk.Label(scrollable_frame, text=f"Seats: {person_data.get('Seats', 'N/A')}").pack(anchor="w", padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Statuses: {person_data.get('Statuses', 'N/A')}").pack(anchor="w", padx=5,
                                                                                           pady=2)
    ttk.Label(scrollable_frame, text=f"Departure Countries: {person_data.get('DepartureCountries', 'N/A')}").pack(
        anchor="w", padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Arrival Countries: {person_data.get('ArrivalCountries', 'N/A')}").pack(
        anchor="w", padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Additional Info: {person_data.get('AdditionalInfos', 'N/A')}").pack(anchor="w",
                                                                                                        padx=5,
                                                                                                        pady=2)
    ttk.Label(scrollable_frame, text=f"Agent Info: {person_data.get('AgentInfos', 'N/A')}").pack(anchor="w", padx=5,
                                                                                               pady=2)
    ttk.Label(scrollable_frame, text=f"Suspicion Score: {suspicion_score:.2f}").pack(anchor="w", padx=5, pady=2)
    ttk.Label(scrollable_frame, text=f"Group: {group}").pack(anchor="w", padx=5, pady=2)


def update_results_display(frame, df, feature_df, suspicion_thresholds, tourist_threshold, regular_traveler_threshold,
                          business_threshold, display_count):
    """Update the display of results in the tkinter window."""
    current_weights = {feat: slider.get() for feat, slider in weight_sliders.items()}
    scores = compute_suspicion_scores(feature_df, current_weights)
    result = df.copy().reset_index(drop=True)
    for col in feature_df.columns:
        result[col] = feature_df[col]
    result["suspicion_score"] = scores
    result["group"] = result.apply(
        lambda row: assign_group(row, suspicion_thresholds, tourist_threshold, regular_traveler_threshold,
                                 business_threshold),
        axis=1)

    # Update list box
    listbox.delete(0, tk.END)  # Clear existing entries
    top = result.sort_values("suspicion_score", ascending=False).head(display_count)
    for _, row in top.iterrows():
        listbox.insert(tk.END,
                       f"{row['PersonID']} - {row['FirstName']} {row['LastName']} - Suspicion: {row['suspicion_score']:.2f} - {row['group']}")


def show_description(feature):
    """Show description for a feature when a button is clicked."""
    descriptions = {
        "flights_count": "Количество полетов: Чем больше полетов, тем выше вероятность подозрительности.",
        "unique_departures_count": "Количество уникальных городов отправления: Меньше городов отправления может свидетельствовать о повторяющихся маршрутах.",
        "unique_arrivals_count": "Количество уникальных городов прибытия: Большее количество городов может указывать на туризм.",
        "repeated_routes_count": "Количество повторяющихся маршрутов: Частые поездки по одним и тем же маршрутам могут указывать на бизнес-путешествия.",
        "same_departure_arrival": "Количество совпадений отправления и прибытия: Если отправление и прибытие совпадают, это может указывать на подозрительную активность."
    }
    description_label.config(text=descriptions.get(feature, "Нет описания для этого параметра"))


def create_interface(df: pd.DataFrame, feature_df: pd.DataFrame):
    """Create the GUI interface using tkinter."""
    global weight_sliders, listbox, description_label

    suspicion_thresholds = {"medium": 40.0, "high": 70.0}

    # Main window
    root = tk.Tk()
    root.title("Suspicion Scoring System")

    # Create frame for sliders and buttons
    frame = ttk.Frame(root)
    frame.pack(padx=10, pady=10)

    # Create sliders for weights
    weight_sliders = {}
    weight_labels = [
        "flights_count", "unique_departures_count", "unique_arrivals_count",
        "repeated_routes_count", "same_departure_arrival"
    ]
    for label in weight_labels:
        # Create a frame for each slider row to align labels and values
        slider_frame = ttk.Frame(frame)
        slider_frame.grid(row=weight_labels.index(label), column=0, columnspan=4, sticky=tk.W, pady=2)

        # Slider value label (displayed to the left of the slider)
        value_label = ttk.Label(slider_frame, text="0.50", width=5)
        value_label.pack(side=tk.LEFT, padx=5)

        # Feature label
        ttk.Label(slider_frame, text=label).pack(side=tk.LEFT, padx=5)

        # Slider
        slider = ttk.Scale(slider_frame, from_=0.0, to=1.0, orient="horizontal", length=200)
        slider.set(0.5)  # Default value
        slider.pack(side=tk.LEFT, padx=5)
        weight_sliders[label] = slider

        # Update value label when slider is moved or released
        def update_value_label(event, slider=slider, label=value_label):
            label.config(text=f"{slider.get():.2f}")

        slider.bind("<B1-Motion>", update_value_label)
        slider.bind("<ButtonRelease-1>", update_value_label)

        # Button for showing descriptions
        description_button = ttk.Button(slider_frame, text="?", command=lambda label=label: show_description(label))
        description_button.pack(side=tk.LEFT, padx=5)

    # Threshold inputs for categories
    ttk.Label(frame, text="Минимум городов для туриста:").grid(row=len(weight_labels), column=0, sticky=tk.W)
    tourist_threshold_entry = ttk.Entry(frame)
    tourist_threshold_entry.insert(0, "5")  # Default value
    tourist_threshold_entry.grid(row=len(weight_labels), column=1)

    ttk.Label(frame, text="Минимум полетов для регулярного путешественника:").grid(row=len(weight_labels) + 1,
                                                                                 column=0, sticky=tk.W)
    regular_traveler_threshold_entry = ttk.Entry(frame)
    regular_traveler_threshold_entry.insert(0, "10")  # Default value
    regular_traveler_threshold_entry.grid(row=len(weight_labels) + 1, column=1)

    ttk.Label(frame, text="Минимум повторяющихся маршрутов для бизнес-путешественника:").grid(
        row=len(weight_labels) + 2, column=0, sticky=tk.W)
    business_threshold_entry = ttk.Entry(frame)
    business_threshold_entry.insert(0, "2")  # Default value
    business_threshold_entry.grid(row=len(weight_labels) + 2, column=1)

    # Textbox for number of people to display
    ttk.Label(frame, text="Количество людей для отображения:").grid(row=len(weight_labels) + 3, column=0, sticky=tk.W)
    display_count_entry = ttk.Entry(frame)
    display_count_entry.insert(0, "100")  # Default value
    display_count_entry.grid(row=len(weight_labels) + 3, column=1)

    # Description label for feature explanation
    description_label = ttk.Label(root, text="", justify=tk.LEFT, wraplength=400)
    description_label.pack(padx=10, pady=10)

    # Listbox to display results
    listbox_frame = ttk.Frame(root)
    listbox_frame.pack(padx=10, pady=10)

    listbox_scrollbar = ttk.Scrollbar(listbox_frame, orient="vertical")
    listbox = tk.Listbox(listbox_frame, width=100, height=10, yscrollcommand=listbox_scrollbar.set)
    listbox.grid(row=0, column=0)
    listbox_scrollbar.config(command=listbox.yview)
    listbox_scrollbar.grid(row=0, column=1, sticky="ns")

    # Bind double-click event to show person details
    listbox.bind("<Double-1>", lambda event: show_person_details(event, df, feature_df, suspicion_thresholds,
                                                                int(tourist_threshold_entry.get()),
                                                                int(regular_traveler_threshold_entry.get()),
                                                                int(business_threshold_entry.get())))

    # Recalculate button
    recalc_button = ttk.Button(root, text="Recalculate",
                               command=lambda: update_results_display(frame, df, feature_df, suspicion_thresholds,
                                                                     int(tourist_threshold_entry.get()),
                                                                     int(regular_traveler_threshold_entry.get()),
                                                                     int(business_threshold_entry.get()),
                                                                     int(display_count_entry.get() or 100)))
    recalc_button.pack(padx=10, pady=10)

    # Export button
    def export_to_csv():
        result = df.copy().reset_index(drop=True)
        for col in feature_df.columns:
            result[col] = feature_df[col]
        result["suspicion_score"] = compute_suspicion_scores(feature_df, {feat: slider.get() for feat, slider in
                                                                         weight_sliders.items()})
        result["group"] = result.apply(
            lambda row: assign_group(row, suspicion_thresholds, int(tourist_threshold_entry.get()),
                                     int(regular_traveler_threshold_entry.get()), int(business_threshold_entry.get())),
            axis=1)
        export_path = os.path.join(os.getcwd(), "suspicious_persons_output.csv")
        result.to_csv(export_path, index=False)
        print(f"Exported full table to {export_path}")

    export_button = ttk.Button(root, text="Export to CSV", command=export_to_csv)
    export_button.pack(padx=10, pady=10)

    # Initial population of the listbox
    update_results_display(frame, df, feature_df, suspicion_thresholds,
                          int(tourist_threshold_entry.get()), int(regular_traveler_threshold_entry.get()),
                          int(business_threshold_entry.get()), int(display_count_entry.get() or 100))

    root.mainloop()


# Main entry point for running the application
def main():
    """Run the application to compute suspicion scores."""
    print("Loading data…")
    df = load_data("persons.csv")
    feature_df = extract_features(df)
    create_interface(df, feature_df)


if __name__ == "__main__":
    main()
#include "../common/ipc.h"
#include "../common/files.h"
#include "../common/product.h"
#include <windows.h>
#undef GetCurrentTime
#include <shellapi.h>
#include <winrt/base.h>
#include <winrt/Windows.Foundation.h>
#include <winrt/Windows.Foundation.Collections.h>
#include <winrt/Windows.Graphics.h>
#include <winrt/Windows.Graphics.Imaging.h>
#include <winrt/Windows.Storage.h>
#include <winrt/Windows.Storage.Streams.h>
#include <winrt/Windows.System.h>
#include <winrt/Windows.UI.Text.h>
#include <winrt/Microsoft.UI.h>
#include <winrt/Microsoft.UI.Interop.h>
#include <winrt/Microsoft.UI.Windowing.h>
#include <winrt/Microsoft.UI.Xaml.h>
#include <winrt/Microsoft.UI.Xaml.Controls.h>
#include <winrt/Microsoft.UI.Xaml.Controls.Primitives.h>
#include <winrt/Microsoft.UI.Xaml.Markup.h>
#include <winrt/Microsoft.UI.Xaml.XamlTypeInfo.h>
#include <winrt/Windows.UI.Xaml.Interop.h>
#include <winrt/Microsoft.UI.Xaml.Input.h>
#include <winrt/Microsoft.UI.Xaml.Media.h>
#include <winrt/Microsoft.UI.Xaml.Media.Imaging.h>
#include <winrt/Microsoft.UI.Xaml.Automation.Peers.h>
#include <winrt/Microsoft.UI.Xaml.Automation.Provider.h>
#include <microsoft.ui.xaml.window.h>
#include "connection_controller.h"
#include "credentials.h"
#include <fstream>
#include <map>
#include <cstdio>

using namespace winrt;
using namespace Microsoft::UI::Xaml;
using namespace Microsoft::UI::Xaml::Controls;
using namespace Microsoft::UI::Xaml::Media;
using namespace Windows::Foundation;
namespace fs=std::filesystem;
using iotvpn::Json;
struct Options { bool test=false; fs::path report,preview; };
Options startupOptions;
int testResult=0;

std::string localTimestamp(const Json& status) {
    const auto text=iotvpn::gui::text(status,"lastSyncAt");
    if(text.empty()) return "尚未同步";
    unsigned year{},month{},day{},hour{},minute{},second{};
    if(sscanf_s(text.c_str(),"%u-%u-%uT%u:%u:%u",&year,&month,&day,&hour,&minute,&second)!=6)
        return "时间不可用";
    SYSTEMTIME utc{},local{};
    utc.wYear=static_cast<WORD>(year); utc.wMonth=static_cast<WORD>(month); utc.wDay=static_cast<WORD>(day);
    utc.wHour=static_cast<WORD>(hour); utc.wMinute=static_cast<WORD>(minute); utc.wSecond=static_cast<WORD>(second);
    if(!SystemTimeToTzSpecificLocalTime(nullptr,&utc,&local)) return "时间不可用";
    char output[32]{};
    sprintf_s(output,"%04u-%02u-%02u %02u:%02u:%02u",local.wYear,local.wMonth,local.wDay,local.wHour,local.wMinute,local.wSecond);
    return output;
}

const wchar_t* layout=LR"XAML(
<Grid xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation" xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml" Background="{ThemeResource ApplicationPageBackgroundThemeBrush}" Padding="32,20,32,24" RowSpacing="24">
 <Grid.RowDefinitions><RowDefinition Height="Auto"/><RowDefinition Height="*"/></Grid.RowDefinitions>
 <Grid ColumnSpacing="12" Height="48">
  <Grid.ColumnDefinitions><ColumnDefinition Width="Auto"/><ColumnDefinition Width="*"/><ColumnDefinition Width="Auto"/></Grid.ColumnDefinitions>
  <Image x:Name="ProductIcon" Width="40" Height="40" AutomationProperties.Name="iot-egine"/>
  <StackPanel Grid.Column="1" VerticalAlignment="Center"><TextBlock Text="iot-egine" Style="{ThemeResource SubtitleTextBlockStyle}"/><TextBlock Text="设备网络" Style="{ThemeResource CaptionTextBlockStyle}" Foreground="{ThemeResource TextFillColorSecondaryBrush}"/></StackPanel>
  <StackPanel Grid.Column="2" Orientation="Horizontal" Spacing="12" VerticalAlignment="Center"><Button x:Name="RepairService" Content="修复服务" Visibility="Collapsed"/><StackPanel x:Name="Account" Orientation="Horizontal" Spacing="12" VerticalAlignment="Center" Visibility="Collapsed"><TextBlock x:Name="AccountName" VerticalAlignment="Center"/><Button x:Name="Logout" Content="退出登录"/></StackPanel></StackPanel>
 </Grid>
 <ScrollViewer x:Name="LoginPanel" Grid.Row="1" VerticalScrollBarVisibility="Auto" HorizontalScrollBarVisibility="Disabled">
  <Border x:Name="LoginCard" MaxWidth="760" Padding="32" HorizontalAlignment="Center" VerticalAlignment="Center" Background="{ThemeResource CardBackgroundFillColorDefaultBrush}" BorderBrush="{ThemeResource CardStrokeColorDefaultBrush}" BorderThickness="1" CornerRadius="12" Margin="0,24">
   <Grid x:Name="LoginColumns" ColumnSpacing="40"><Grid.ColumnDefinitions><ColumnDefinition Width="240"/><ColumnDefinition Width="*"/></Grid.ColumnDefinitions>
    <StackPanel x:Name="Introduction" Spacing="20" VerticalAlignment="Center"><FontIcon Glyph="&#xE968;" FontSize="56" HorizontalAlignment="Left" Foreground="{ThemeResource AccentTextFillColorPrimaryBrush}"/><TextBlock Text="连接你的设备" Style="{ThemeResource TitleTextBlockStyle}"/><TextBlock Text="一个账号，随时接入。&#xA;关闭窗口后，连接继续运行。" TextWrapping="Wrap" Foreground="{ThemeResource TextFillColorSecondaryBrush}"/></StackPanel>
    <StackPanel Grid.Column="1" Spacing="16"><TextBlock Text="欢迎回来" Style="{ThemeResource TitleTextBlockStyle}"/><TextBlock Text="登录后选择需要连接的设备" Foreground="{ThemeResource TextFillColorSecondaryBrush}" TextWrapping="Wrap"/>
     <TextBox x:Name="Username" Header="用户名" PlaceholderText="请输入用户名" MaxLength="255"/>
     <PasswordBox x:Name="Password" Header="密码" PlaceholderText="请输入密码" MaxLength="511" PasswordRevealMode="Peek"/>
     <CheckBox x:Name="Remember" Content="记住用户名和密码" IsChecked="True"/>
     <Button x:Name="Login" Content="登录" Style="{ThemeResource AccentButtonStyle}" HorizontalAlignment="Stretch"/>
     <TextBlock Text="凭据由 Windows 加密保存" Style="{ThemeResource CaptionTextBlockStyle}" Foreground="{ThemeResource TextFillColorSecondaryBrush}"/>
     <InfoBar x:Name="LoginMessage" IsOpen="False" IsClosable="False" Severity="Warning"/>
    </StackPanel>
   </Grid>
  </Border>
 </ScrollViewer>
 <Grid x:Name="DevicesPanel" Grid.Row="1" Visibility="Collapsed" RowSpacing="16">
  <Grid.RowDefinitions><RowDefinition Height="Auto"/><RowDefinition Height="Auto"/><RowDefinition Height="Auto"/><RowDefinition Height="*"/><RowDefinition Height="Auto"/></Grid.RowDefinitions>
  <Border Background="{ThemeResource CardBackgroundFillColorDefaultBrush}" BorderBrush="{ThemeResource CardStrokeColorDefaultBrush}" BorderThickness="1" CornerRadius="8" Padding="20,16"><StackPanel Orientation="Horizontal" Spacing="28"><TextBlock x:Name="NetworkState" Text="已登录" Style="{ThemeResource BodyStrongTextBlockStyle}"/><TextBlock x:Name="Address"/><TextBlock x:Name="SyncTime" Foreground="{ThemeResource TextFillColorSecondaryBrush}"/></StackPanel></Border>
  <Grid Grid.Row="1" ColumnSpacing="8">
   <Grid.ColumnDefinitions><ColumnDefinition Width="*"/><ColumnDefinition Width="Auto"/><ColumnDefinition Width="Auto"/><ColumnDefinition Width="Auto"/></Grid.ColumnDefinitions>
   <TextBox x:Name="Search" PlaceholderText="搜索设备名称或 IMEI" VerticalAlignment="Center"/>
   <Button x:Name="Refresh" Grid.Column="1" Content="刷新"/>
   <Button x:Name="Sync" Grid.Column="2" Content="同步配置"/>
   <StackPanel Grid.Column="3" Orientation="Horizontal" Spacing="8" VerticalAlignment="Center">
    <TextBlock Text="VPN" VerticalAlignment="Center"/>
    <ToggleSwitch x:Name="VpnEnabled" AutomationProperties.Name="VPN" OnContent="已开启" OffContent="已关闭" VerticalAlignment="Center"/>
   </StackPanel>
  </Grid>
  <InfoBar x:Name="DeviceMessage" Grid.Row="2" IsOpen="False" IsClosable="False" Severity="Warning"/>
  <Border Grid.Row="3" Background="{ThemeResource CardBackgroundFillColorDefaultBrush}" BorderBrush="{ThemeResource CardStrokeColorDefaultBrush}" BorderThickness="1" CornerRadius="8"><Grid><ListView x:Name="DeviceList" SelectionMode="None" Padding="8"/><TextBlock x:Name="Empty" Text="正在读取设备…" HorizontalAlignment="Center" VerticalAlignment="Center" Foreground="{ThemeResource TextFillColorSecondaryBrush}"/></Grid></Border>
  <Grid Grid.Row="4"><StackPanel Spacing="4"><TextBlock x:Name="Selection" Text="已选择 0 台"/><TextBlock x:Name="Hint" Text="关闭窗口后，连接继续运行" Style="{ThemeResource CaptionTextBlockStyle}" Foreground="{ThemeResource TextFillColorSecondaryBrush}"/></StackPanel><Button x:Name="Apply" Content="应用选择" HorizontalAlignment="Right" VerticalAlignment="Center" Style="{ThemeResource AccentButtonStyle}"/></Grid>
 </Grid>
</Grid>)XAML";

struct App : ApplicationT<App,Markup::IXamlMetadataProvider> {
    XamlTypeInfo::XamlControlsXamlMetaDataProvider provider;
    Markup::IXamlType GetXamlType(hstring const& name) { return provider.GetXamlType(name); }
    Markup::IXamlType GetXamlType(Windows::UI::Xaml::Interop::TypeName const& type) { return provider.GetXamlType(type); }
    com_array<Markup::XmlnsDefinition> GetXmlnsDefinitions() { return provider.GetXmlnsDefinitions(); }
    Window window{nullptr}; Grid root{nullptr}; DispatcherTimer timer{nullptr};
    ContentDialog logoutDialog{nullptr}; bool logoutConfirming=false, repairing=false;
    std::unique_ptr<iotvpn::gui::ConnectionController> model;
    std::string rendered; bool updating=false,closed=false;
    template<class T> T control(const wchar_t* name) { return root.FindName(name).as<T>(); }
    Button findDialogButton(DependencyObject const& node,hstring const& label) {
        if(auto button=node.try_as<Button>();button&&unbox_value_or<hstring>(button.Content(),L"")==label) return button;
        for(int i=0;i<VisualTreeHelper::GetChildrenCount(node);++i)
            if(auto found=findDialogButton(VisualTreeHelper::GetChild(node,i),label)) return found;
        return nullptr;
    }
    void clickDialogButton(hstring const& label) {
        auto button=findDialogButton(logoutDialog,label);
        if(!button) throw std::runtime_error("Logout confirmation button is missing");
        Microsoft::UI::Xaml::Automation::Peers::ButtonAutomationPeer peer(button);
        peer.GetPattern(Microsoft::UI::Xaml::Automation::Peers::PatternInterface::Invoke)
            .as<Microsoft::UI::Xaml::Automation::Provider::IInvokeProvider>().Invoke();
    }
    Json fixtureState={{"state","Connected"},{"username","demo"},{"assignedIpv4","100.96.0.8"},{"vpnEnabled",true},{"tunnelRunning",true},{"edgeNodeIds",Json::array({"11111111-1111-4111-8111-111111111111"})}};
    Json fixture(const Json& request) {
        const auto command=request.value("command","");
        if(command=="logout") fixtureState={{"state","LoggedOut"},{"vpnEnabled",false}};
        if(command=="apply"||command=="connect") {
            fixtureState["edgeNodeIds"]=request.at("edgeNodeIds");
            fixtureState["vpnEnabled"]=command=="connect"||request.value("vpnEnabled",true);
            fixtureState["tunnelRunning"]=fixtureState["vpnEnabled"];
            fixtureState["state"]=fixtureState["vpnEnabled"].get<bool>()?"Connected":"Disconnected";
        }
        if(command=="disconnect") { fixtureState["state"]="Disconnected"; fixtureState["tunnelRunning"]=false; fixtureState["vpnEnabled"]=false; }
        return {{"success",true},{"status",fixtureState},{"devices",Json::array({
            {{"id","11111111-1111-4111-8111-111111111111"},{"name","东区泵站"},{"imei","867530900001001"},{"online",true},{"virtualCidrs",{"172.24.1.0/24"}},{"assignedIpv4","100.96.0.2"}},
            {{"id","22222222-2222-4222-8222-222222222222"},{"name","西区闸门"},{"imei","867530900001002"},{"online",false},{"virtualCidrs",{"172.24.2.0/24"}},{"assignedIpv4","100.96.0.5"}}
        })}};
    }
    void OnLaunched(LaunchActivatedEventArgs const&) {
        try {
            Resources().MergedDictionaries().Append(XamlControlsResources());
            root=Markup::XamlReader::Load(layout).as<Grid>();
            control<Image>(L"ProductIcon").Source(Media::Imaging::SvgImageSource(Uri(L"file:///"+(iotvpn::moduleDirectory()/L"product.svg").generic_wstring())));
            model=std::make_unique<iotvpn::gui::ConnectionController>([this](const Json& request,std::stop_token stop) {
                if(startupOptions.test) return fixture(request);
                try {
                    auto result=iotvpn::pipeRequest(request,90000,stop);
                    if(request.value("command","")=="login" && result.value("success",false)) {
                        try { if(request.value("rememberCredentials",false)) iotvpn::gui::Credentials().save(request.at("username").get_ref<const std::string&>(),request.at("password").get_ref<const std::string&>()); else iotvpn::gui::Credentials().clear(); }
                        catch(...) { result["message"]="登录成功，但保存账号失败，请重试。"; }
                    }
                    return result;
                } catch(const iotvpn::LocalServiceError& error) {
                    return Json{{"success",false},{"message",error.what()},{"serviceRepair",true}};
                }
            },startupOptions.test);
            window=Window(); window.Title(L"iot-egine"); window.Content(root);
            const auto icon=LoadIconW(GetModuleHandleW(nullptr),MAKEINTRESOURCEW(1));
            if(!icon) throw_last_error();
            window.AppWindow().SetIcon(Microsoft::UI::GetIconIdFromIcon(icon));
            window.SystemBackdrop(MicaBackdrop());
            auto presenter=window.AppWindow().Presenter().as<Microsoft::UI::Windowing::OverlappedPresenter>();
            HWND handle{}; check_hresult(window.as<IWindowNative>()->get_WindowHandle(&handle));
            const auto dpi=GetDpiForWindow(handle); const auto pixels=[dpi](int value){return MulDiv(value,static_cast<int>(dpi),96);};
            presenter.PreferredMinimumWidth(pixels(700)); presenter.PreferredMinimumHeight(pixels(560));
            MONITORINFO monitor{sizeof(monitor)}; GetMonitorInfoW(MonitorFromWindow(handle,MONITOR_DEFAULTTONEAREST),&monitor);
            window.AppWindow().Resize({std::min(pixels(1040),static_cast<int>(monitor.rcWork.right-monitor.rcWork.left)),std::min(pixels(720),static_cast<int>(monitor.rcWork.bottom-monitor.rcWork.top))});
            root.SizeChanged([this](auto const&,auto const&) {
                const bool compact=root.ActualWidth()<760;
                control<Border>(L"LoginCard").Width(std::max(280.0,std::min(compact?440.0:760.0,root.ActualWidth()-64.0)));
                control<StackPanel>(L"Introduction").Visibility(compact?Visibility::Collapsed:Visibility::Visible);
                control<Grid>(L"LoginColumns").ColumnDefinitions().GetAt(0).Width({compact?0.0:240.0,GridUnitType::Pixel});
                control<Grid>(L"LoginColumns").ColumnSpacing(compact?0:40);
            });
            window.Closed([this](auto const&,auto const&) { closed=true; if(timer) timer.Stop(); model.reset(); });
            control<Button>(L"Login").Click([this](auto const&,auto const&) { login(); });
            control<Button>(L"RepairService").Click([this](auto const&,auto const&) { repairService(); });
            control<PasswordBox>(L"Password").KeyDown([this](auto const&,Microsoft::UI::Xaml::Input::KeyRoutedEventArgs const& e) { if(e.Key()==Windows::System::VirtualKey::Enter) { login(); e.Handled(true); } });
            control<Button>(L"Logout").Click([this](auto const&,auto const&) { confirmLogout(); });
            for(auto [name,command]:{std::pair{L"Refresh","devices"},{L"Sync","sync"},{L"Apply","apply"}})
                control<Button>(name).Click([this,command](auto const&,auto const&) { model->request(command); render(); });
            control<ToggleSwitch>(L"VpnEnabled").Toggled([this](auto const&,auto const&) { if(!updating) { model->request(control<ToggleSwitch>(L"VpnEnabled").IsOn()?"connect":"disconnect"); render(); } });
            control<TextBox>(L"Search").TextChanged([this](auto const&,auto const&) { rendered.clear(); render(); });
            control<CheckBox>(L"Remember").Unchecked([this](auto const&,auto const&) { if(!startupOptions.test) { try { iotvpn::gui::Credentials().clear(); } catch(...) { model->message="无法清除已保存的账号，请重试。"; } } });
            if(!startupOptions.test) {
                try { std::string user,password; if(iotvpn::gui::Credentials().load(user,password)) { control<TextBox>(L"Username").Text(to_hstring(user)); control<PasswordBox>(L"Password").Password(to_hstring(password)); } SecureZeroMemory(password.data(),password.size()); }
                catch(...) { model->message="无法读取已保存的账号，请重新输入。"; }
                model->request("status");
            }
            timer=DispatcherTimer(); timer.Interval(std::chrono::milliseconds(100));
            timer.Tick([this](auto const&,auto const&) { if(model) { model->tick(!startupOptions.test&&!logoutConfirming); render(); } }); timer.Start();
            window.Activate(); render();
            if(startupOptions.test) selfTest();
        } catch(hresult_error const& error) { fail(to_string(error.message())); }
        catch(std::exception const& error) { fail(error.what()); }
    }
    void fail(std::string const& message) {
        testResult=1;
        if(!startupOptions.report.empty()) std::ofstream(startupOptions.report)<<"FAIL "<<message;
        if(!startupOptions.test) MessageBoxW(nullptr,to_hstring(message).c_str(),L"iot-egine",MB_OK|MB_ICONERROR);
        if(window) window.Close(); else Exit();
    }
    fire_and_forget confirmLogout() {
        auto lifetime=get_strong();
        if(!model||model->busy||logoutConfirming||repairing) co_return;
        logoutConfirming=true;
        try {
            logoutDialog=ContentDialog(); logoutDialog.XamlRoot(root.XamlRoot());
            logoutDialog.Title(box_value(L"确认退出登录？"));
            logoutDialog.Content(box_value(L"退出后会断开本机设备网络并停止配置同步。\n如果希望后台继续连接，只需关闭窗口。"));
            logoutDialog.PrimaryButtonText(L"退出登录"); logoutDialog.CloseButtonText(L"取消");
            logoutDialog.DefaultButton(ContentDialogButton::Close);
            const auto result=co_await logoutDialog.ShowAsync();
            if(!closed&&model&&result==ContentDialogResult::Primary) { model->request("logout"); render(); }
        } catch(hresult_error const&) {
            if(!closed&&model) { model->message="无法显示退出确认，请重试。"; render(); }
        }
        logoutDialog=nullptr; logoutConfirming=false;
    }
    fire_and_forget repairService() {
        auto lifetime=get_strong();
        if(!model||model->busy||repairing||startupOptions.test) co_return;
        repairing=true;
        model->message="正在修复本机服务，请允许管理员权限。";
        render();
        apartment_context ui;
        iotvpn::ServiceRepairResult result;
        bool failed=false;
        co_await winrt::resume_background();
        try { result=iotvpn::repairLocalService(); } catch(...) { failed=true; }
        co_await ui;
        if(failed) result={false,false,"服务修复失败，请稍后重试。"};
        repairing=false;
        if(closed||!model) co_return;
        model->message=result.message;
        model->serviceRepairNeeded=!result.succeeded;
        if(result.succeeded) model->request("status");
        render();
    }
    void login() {
        if(model->busy||repairing) return;
        auto user=to_string(control<TextBox>(L"Username").Text()),password=to_string(control<PasswordBox>(L"Password").Password());
        const auto remember=control<CheckBox>(L"Remember").IsChecked().GetBoolean();
        if(user.empty()||password.empty()) model->message="请输入用户名和密码。";
        else { model->request("login",{{"username",user},{"password",password},{"serverUrl",iotvpn::PlatformUrl},{"rememberCredentials",remember}}); if(!remember) control<PasswordBox>(L"Password").Password(L""); }
        SecureZeroMemory(password.data(),password.size()); render();
    }
    void render() {
        if(updating||!model||closed) return; updating=true;
        struct Guard { bool& flag; ~Guard(){flag=false;} } guard{updating};
        const bool logged=model->loggedIn;
        control<ScrollViewer>(L"LoginPanel").Visibility(logged?Visibility::Collapsed:Visibility::Visible);
        control<Grid>(L"DevicesPanel").Visibility(logged?Visibility::Visible:Visibility::Collapsed);
        control<StackPanel>(L"Account").Visibility(logged?Visibility::Visible:Visibility::Collapsed);
        control<TextBlock>(L"AccountName").Text(to_hstring(model->username));
        for(const auto name:{L"Login",L"Logout",L"Refresh",L"Sync",L"Apply",L"RepairService"}) control<Button>(name).IsEnabled(!model->busy&&!repairing);
        control<Button>(L"RepairService").Visibility(model->serviceRepairNeeded?Visibility::Visible:Visibility::Collapsed);
        control<Button>(L"RepairService").Content(box_value(repairing?L"正在修复…":L"修复服务"));
        control<TextBox>(L"Username").IsEnabled(!model->busy&&!repairing); control<PasswordBox>(L"Password").IsEnabled(!model->busy&&!repairing); control<CheckBox>(L"Remember").IsEnabled(!model->busy&&!repairing);
        control<Button>(L"Apply").IsEnabled(!model->busy&&!repairing&&model->selected.size()<=64);
        control<Button>(L"Login").Content(box_value(model->busy?L"正在登录…":L"登录"));
        for(const auto name:{L"LoginMessage",L"DeviceMessage"}) { auto bar=control<InfoBar>(name); bar.Message(to_hstring(model->message)); bar.IsOpen(!model->message.empty()); }
        control<ToggleSwitch>(L"VpnEnabled").IsEnabled(!model->busy&&!repairing&&(model->vpnEnabled||(!model->selected.empty()&&model->selected.size()<=64))); control<ToggleSwitch>(L"VpnEnabled").IsOn(model->vpnEnabled);
        const auto state=iotvpn::gui::text(model->status,"state");
        const std::map<std::string,std::wstring> labels{{"Connected",L"已连接"},{"Disconnected",L"已断开"},{"Authenticated",L"已登录"},{"Connecting",L"连接中"},{"Retrying",L"正在重试"},{"Revoked",L"授权已撤销"},{"Error",L"连接异常"}};
        control<TextBlock>(L"NetworkState").Text(labels.contains(state)?hstring(labels.at(state)):to_hstring(state));
        const auto assignedAddress=iotvpn::gui::text(model->status,"assignedIpv4");
        control<TextBlock>(L"Address").Text(to_hstring("本机地址  "+(assignedAddress.empty()?std::string("尚未分配"):assignedAddress)));
        control<TextBlock>(L"SyncTime").Text(to_hstring("同步  "+localTimestamp(model->status)));
        control<TextBlock>(L"Selection").Text(to_hstring("已选择 "+std::to_string(model->selected.size())+" 台"+(model->changed()?" · 待应用":"")));
        control<TextBlock>(L"Hint").Text(model->busy||repairing?L"正在处理…":model->vpnEnabled?L"关闭窗口后，VPN 继续运行":L"VPN 已关闭，所选设备和虚拟网段保留");
        auto query=to_string(control<TextBox>(L"Search").Text()); const auto devices=model->filtered(query);
        std::string signature=query; for(const auto& d:devices) signature+=d.id+d.name+d.imei+d.subnet+d.address+(d.online?"1":"0")+(model->selected.contains(d.id)?"1":"0");
        if(signature!=rendered) {
            rendered=signature; auto list=control<ListView>(L"DeviceList"); list.Items().Clear();
            for(const auto& d:devices) {
                CheckBox row; row.HorizontalAlignment(HorizontalAlignment::Stretch); row.HorizontalContentAlignment(HorizontalAlignment::Stretch); row.Padding({12,12,12,12});
                StackPanel content; content.Spacing(5);
                TextBlock name; name.Text(to_hstring(d.name+(d.online?"  ·  在线":"  ·  离线"))); name.FontWeight(Windows::UI::Text::FontWeights::SemiBold()); content.Children().Append(name);
                TextBlock info; info.Text(to_hstring("IMEI "+d.imei+"    虚拟子网 "+d.subnet+"    设备地址 "+d.address)); info.TextWrapping(TextWrapping::Wrap); info.Opacity(0.7); content.Children().Append(info);
                row.Content(content); row.IsChecked(model->selected.contains(d.id));
                row.Checked([this,id=d.id](auto const&,auto const&) { if(!updating) { model->choose(id,true); render(); } });
                row.Unchecked([this,id=d.id](auto const&,auto const&) { if(!updating) { model->choose(id,false); render(); } });
                list.Items().Append(row);
            }
        }
        control<TextBlock>(L"Empty").Visibility(devices.empty()?Visibility::Visible:Visibility::Collapsed);
        control<TextBlock>(L"Empty").Text(model->busy?L"正在读取设备…":model->devices.empty()?L"暂无可用设备":L"没有匹配的设备");
    }
    Windows::Foundation::IAsyncAction capture(std::wstring name) {
        if(startupOptions.preview.empty()) co_return;
        Media::Imaging::RenderTargetBitmap bitmap;
        co_await bitmap.RenderAsync(root);
        const auto buffer=co_await bitmap.GetPixelsAsync();
        auto folder=co_await Windows::Storage::StorageFolder::GetFolderFromPathAsync(startupOptions.preview.wstring());
        auto file=co_await folder.CreateFileAsync(name,Windows::Storage::CreationCollisionOption::ReplaceExisting);
        auto stream=co_await file.OpenAsync(Windows::Storage::FileAccessMode::ReadWrite);
        auto encoder=co_await Windows::Graphics::Imaging::BitmapEncoder::CreateAsync(Windows::Graphics::Imaging::BitmapEncoder::PngEncoderId(),stream);
        auto reader=Windows::Storage::Streams::DataReader::FromBuffer(buffer); std::vector<uint8_t> pixels(buffer.Length()); reader.ReadBytes(pixels);
        encoder.SetPixelData(Windows::Graphics::Imaging::BitmapPixelFormat::Bgra8,Windows::Graphics::Imaging::BitmapAlphaMode::Premultiplied,bitmap.PixelWidth(),bitmap.PixelHeight(),96,96,pixels);
        co_await encoder.FlushAsync();
    }
    fire_and_forget selfTest() {
        auto lifetime=get_strong();
        apartment_context ui;
        try {
            co_await resume_after(std::chrono::milliseconds(600));
            co_await ui;
            co_await capture(L"login.png");
            control<TextBox>(L"Username").Text(L"demo"); control<PasswordBox>(L"Password").Password(L"test-only"); login();
            model->request("devices"); render();
            if(model->devices.size()!=2) throw std::runtime_error("WinUI login and device load failed");
            co_await resume_after(std::chrono::milliseconds(300)); co_await ui;
            co_await capture(L"devices.png");
            control<ToggleSwitch>(L"VpnEnabled").IsOn(false);
            if(model->vpnEnabled||model->selected.size()!=1) throw std::runtime_error("VPN off lost the saved selection");
            model->request("apply"); render();
            if(model->vpnEnabled||fixtureState.value("tunnelRunning",true)) throw std::runtime_error("Saving while off started VPN");
            co_await capture(L"vpn-off.png");
            control<ToggleSwitch>(L"VpnEnabled").IsOn(true);
            if(!model->vpnEnabled||!fixtureState.value("tunnelRunning",false)) throw std::runtime_error("VPN on did not connect");
            auto first=control<ListView>(L"DeviceList").Items().GetAt(0).as<CheckBox>(); first.IsChecked(false);
            if(!model->selected.empty()) throw std::runtime_error("WinUI checkbox selection failed");
            model->request("apply"); render();
            if(!model->applied.empty()) throw std::runtime_error("WinUI apply failed");
            control<TextBox>(L"Search").Text(L"西区");
            co_await resume_after(std::chrono::milliseconds(150)); co_await ui;
            render();
            if(control<ListView>(L"DeviceList").Items().Size()!=1) throw std::runtime_error("WinUI search failed");
            confirmLogout();
            co_await resume_after(std::chrono::milliseconds(350)); co_await ui;
            co_await capture(L"logout-confirm.png");
            clickDialogButton(L"取消");
            co_await resume_after(std::chrono::milliseconds(350)); co_await ui;
            if(!model->loggedIn||logoutConfirming) throw std::runtime_error("Cancelling logout changed the session");
            confirmLogout();
            co_await resume_after(std::chrono::milliseconds(350)); co_await ui;
            clickDialogButton(L"退出登录");
            co_await resume_after(std::chrono::milliseconds(350)); co_await ui;
            if(model->loggedIn) throw std::runtime_error("WinUI logout failed");
            window.AppWindow().Resize({820,640});
            co_await resume_after(std::chrono::milliseconds(300)); co_await ui;
            co_await capture(L"login-compact.png");
            if(!startupOptions.report.empty()) std::ofstream(startupOptions.report)<<"PASS C++ WinUI 3 Fluent controls, login, VPN toggle with saved selection, devices, apply, search, cancelled logout and confirmed logout\n";
            window.Close();
        } catch(hresult_error const& error) { fail(to_string(error.message())); }
        catch(std::exception const& error) { fail(error.what()); }
    }
};
int WINAPI wWinMain(HINSTANCE,HINSTANCE,PWSTR,int) {
    int argc{}; auto argv=CommandLineToArgvW(GetCommandLineW(),&argc);
    for(int i=1;i<argc;++i) { const std::wstring arg=argv[i]; if(arg==L"--self-test") startupOptions.test=true; else if(arg==L"--report"&&i+1<argc) startupOptions.report=argv[++i]; else if(arg==L"--preview-dir"&&i+1<argc) startupOptions.preview=fs::absolute(argv[++i]); }
    LocalFree(argv); if(!startupOptions.preview.empty()) fs::create_directories(startupOptions.preview);
    init_apartment(apartment_type::single_threaded);
    Application::Start([](auto&&){make<App>();});
    return testResult;
}
